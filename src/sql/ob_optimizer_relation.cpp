
#include "ob_optimizer_relation.h"
#include <math.h>
#include <float.h>

namespace oceanbase
{
  namespace sql
  {

    /*
     * clamp_row_est
     *		Force a row-count estimate to a sane value.
     */
    double ObJoinStatCalculator::clamp_row_est(double nrows)
    {
      /*
       * Force estimate to be at least one row, to make explain output look
       * better and to avoid possible divide-by-zero when interpolating costs.
       * Make it an integer, too.
       */
      if (nrows <= 1.0)
        nrows = 1.0;
      else
        nrows = rint(nrows);

      return nrows;
    }

    double ObJoinStatCalculator::rint(double x)
    {
      return (x >= 0.0) ? floor(x + 0.5) : ceil(x - 0.5);
    }

    /*
     * calc_joinrel_size_estimate
     *		Workhorse for set_joinrel_size_estimates and
     *		get_parameterized_joinrel_size.
     */
    double ObJoinStatCalculator::calc_joinrel_size_estimate(ObLogicalPlan *logical_plan,
                   ObSelectStmt *select_stmt,
                   const uint64_t join_type,
                   const double outer_rows,
                   const double inner_rows,
                   oceanbase::common::ObList<ObSqlRawExpr*>& restrictlist)
    {
      UNUSED(select_stmt);
      UNUSED(logical_plan);
//      UNUSED(restrictlist);
//      SpecialJoinInfo *sjinfo;
//      JoinType	jointype = sjinfo->jointype;
      Selectivity jselec = 0.5;
      Selectivity pselec = 0.5;
      double		nrows;

      TBSYS_LOG(ERROR,"dhc start estimate  restrictlist.size=%ld",restrictlist.size());

      /*
       * Compute joinclause selectivity.	Note that we are only considering
       * clauses that become restriction clauses at this join level; we are not
       * double-counting them because they were not considered in estimating the
       * sizes of the component rels.
       *
       * For an outer join, we have to distinguish the selectivity of the join's
       * own clauses (JOIN/ON conditions) from any clauses that were "pushed
       * down".  For inner joins we just count them all as joinclauses.
       */
//      if (IS_OUTER_JOIN(jointype))
//      {
//        List	   *joinquals = null;
//        List	   *pushedquals = null;
//        ListCell   *l;

//        /* Grovel through the clauses to separate into two lists */
//        foreach(l, restrictlist)
//        {
//          RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

//          Assert(IsA(rinfo, RestrictInfo));
//          if (rinfo->is_pushed_down)
//            pushedquals = lappend(pushedquals, rinfo);
//          else
//            joinquals = lappend(joinquals, rinfo);
//        }

//        /* Get the separate selectivities */
//        jselec = clauselist_selectivity(root,
//                        joinquals,
//                        0,
//                        jointype,
//                        sjinfo);
//        pselec = clauselist_selectivity(root,
//                        pushedquals,
//                        0,
//                        jointype,
//                        sjinfo);

//        /* Avoid leaking a lot of ListCells */
//        list_free(joinquals);
//        list_free(pushedquals);
//      }
//      else
//      {
/*        jselec = clauselist_selectivity(logical_plan,
                        select_stmt,
                        join_type,
                        restrictlist
                        );
        pselec = 0.0;	*/		/* not used, keep compiler quiet */
//      }

      /*
       * Basically, we multiply size of Cartesian product by selectivity.
       *
       * If we are doing an outer join, take that into account: the joinqual
       * selectivity has to be clamped using the knowledge that the output must
       * be at least as large as the non-nullable input.	However, any
       * pushed-down quals are applied after the outer join, so their
       * selectivity applies fully.
       *
       * For JOIN_SEMI and JOIN_ANTI, the selectivity is defined as the fraction
       * of LHS rows that have matches, and we apply that straightforwardly.
       */
      switch (join_type)
      {
        case JoinedTable::T_FULL:
          nrows = outer_rows * inner_rows * jselec;
          if (nrows < outer_rows)
            nrows = outer_rows;
          if (nrows < inner_rows)
            nrows = inner_rows;
          nrows *= pselec;
          break;
        case JoinedTable::T_RIGHT://add dhc,need updata,pg need not solve it
        case JoinedTable::T_LEFT:
          nrows = outer_rows * inner_rows * jselec;
          if (nrows < outer_rows)
            nrows = outer_rows;
          nrows *= pselec;
          break;

        case JoinedTable::T_INNER:
          nrows = outer_rows * inner_rows * jselec;
          break;
//        case JOIN_SEMI:
//          nrows = outer_rows * jselec;
//          /* pselec not used */
//          break;
//        case JOIN_ANTI:
//          nrows = outer_rows * (1.0 - jselec);
//          nrows *= pselec;
//          break;
        default:
          /* other values not expected here */
          nrows = 0;			/* keep compiler quiet */
          break;
      }

      TBSYS_LOG(ERROR,"DHC end calc_joinrel_size_estimate=%lf",nrows);

      return clamp_row_est(nrows);
    }


    /****************************************************************************
     *		ROUTINES TO COMPUTE SELECTIVITIES
     ****************************************************************************/

    /*
     * clauselist_selectivity -
     *	  Compute the selectivity of an implicitly-ANDed list of boolean
     *	  expression clauses.  The list can be empty, in which case 1.0
     *	  must be returned.  List elements may be either RestrictInfos
     *	  or bare expression clauses --- the former is preferred since
     *	  it allows caching of results.
     *
     * See clause_selectivity() for the meaning of the additional parameters.
     *
     * Our basic approach is to take the product of the selectivities of the
     * subclauses.	However, that's only right if the subclauses have independent
     * probabilities, and in reality they are often NOT independent.  So,
     * we want to be smarter where we can.

     * Currently, the only extra smarts we have is to recognize "range queries",
     * such as "x > 34 AND x < 42".  Clauses are recognized as possible range
     * query components if they are restriction opclauses whose operators have
     * scalarltsel() or scalargtsel() as their restriction selectivity estimator.
     * We pair up clauses of this form that refer to the same variable.  An
     * unpairable clause of this kind is simply multiplied into the selectivity
     * product in the normal way.  But when we find a pair, we know that the
     * selectivities represent the relative positions of the low and high bounds
     * within the column's range, so instead of figuring the selectivity as
     * hisel * losel, we can figure it as hisel + losel - 1.  (To visualize this,
     * see that hisel is the fraction of the range below the high bound, while
     * losel is the fraction above the low bound; so hisel can be interpreted
     * directly as a 0..1 value but we need to convert losel to 1-losel before
     * interpreting it as a value.	Then the available range is 1-losel to hisel.
     * However, this calculation double-excludes nulls, so really we need
     * hisel + losel + null_frac - 1.)
     *
     * If either selectivity is exactly DEFAULT_INEQ_SEL, we forget this equation
     * and instead use DEFAULT_RANGE_INEQ_SEL.	The same applies if the equation
     * yields an impossible (negative) result.
     *
     * A free side-effect is that we can recognize redundant inequalities such
     * as "x < 4 AND x < 5"; only the tighter constraint will be counted.
     *
     * Of course this is all very dependent on the behavior of
     * scalarltsel/scalargtsel; perhaps some day we can generalize the approach.
     */
    Selectivity
    ObJoinStatCalculator::clauselist_selectivity(ObLogicalPlan *logical_plan,
      ObSelectStmt *select_stmt,
      const uint64_t join_type,
      oceanbase::common::ObList<ObSqlRawExpr*>& restrictlist)
    {
      UNUSED(logical_plan);
      UNUSED(select_stmt);
      UNUSED(join_type);
      UNUSED(restrictlist);
      Selectivity s1 = 1.0;

      return s1;
    }

    int cast_obj_to_exprObj(char *buf,ObObj &obj,
                            const ObObj &value,
                            ObExprObj &e_value)
    {
      int ret = OB_SUCCESS;
      obj.set_type(value.get_type());
      //char buf[OB_MAX_VARCHAR_LENGTH];
      buf[0] = '\0';
      if (value.get_type() == ObVarcharType)
      {
        ObString str,cstr;
        value.get_varchar(str);
        str.to_string(buf,str.length()+1);
        buf[str.length()]= '\0';

        cstr.assign(buf,str.length()+1);
        obj.set_varchar(cstr);
        TBSYS_LOG(DEBUG,"QX cast obj %s",to_cstring(obj));
        e_value.assign(obj);
        //TBSYS_LOG(DEBUG,"QX assign obj %s",print_obj(tobj));
      }
      else
      {
        e_value.assign(value);
      }
      return ret;
    }

    bool ObStatSelCalculator::is_unique_rowkey_column(ObOptimizerRelation *rel_opt ,
                                                      const uint64_t table_id,
                                                      const uint64_t column_id)
    {
      bool unique_rowkey_column = false;
      //table id may is alias table id
      UNUSED(table_id);
      uint64_t table_ref_id = rel_opt->get_table_ref_id();
      uint64_t c_id = 0;
      const common::ObSchemaManagerV2 *schema_managerv2 = rel_opt->get_schema_managerv2();
      const common::ObTableSchema * table_schema = NULL;
      if (rel_opt->get_table_id() != table_id) //table don't own the expr
      {
      }
      else if (schema_managerv2 == NULL)
      {
        TBSYS_LOG(ERROR,"QX schema_managerv2 is null.");
      }
      else
      {
        table_schema = schema_managerv2->get_table_schema(table_ref_id);
      }

      if (rel_opt->get_table_id() != table_id) //table don't own the expr
      {
      }
      else if (table_schema == NULL)
      {
        TBSYS_LOG(ERROR,"QX　table_schema == NULL.");
      }
      else
      {
        //rowkey info
        common::ObRowkeyInfo rowkey_info = table_schema->get_rowkey_info();
        if (rowkey_info.get_size() != 1)
        {
        }
        else if (OB_SUCCESS == rowkey_info.get_column_id(0,c_id) && c_id == column_id)
        {
          unique_rowkey_column = true;
        }
      }
      return unique_rowkey_column;
    }

    double ObStatSelCalculator::get_equal_selectivity(ObOptimizerRelation *rel_opt ,
                                                      ObSelInfo &sel_info,
                                                      const oceanbase::common::ObObj &value)
    {
      double  sel = 0.0;
      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;
      int  ret = OB_SUCCESS;
      TBSYS_LOG(DEBUG,"rel_opt.table_id =%ld,sel_info.table_id=%ld,sel.info.column_id=%ld",
                rel_opt->get_table_id(),table_id,column_id);

      common::ObExprObj e_value;
      // specail processing varchar
      // constrawexpr without \0
      ObObj obj;
      char buf[OB_MAX_VARCHAR_LENGTH/32];
      cast_obj_to_exprObj(buf,obj,value,e_value);
      //e_value.assign(value);
      int cmp = 0;
      bool flag = false;

      //TBSYS_LOG(DEBUG,"QX value type = %d v = %s",value.get_type(),print_obj(value));

      if (rel_opt->get_table_id() != table_id) //table don't own the expr
      {
        sel = 1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else
      {
        bool unique_column = is_unique_rowkey_column(rel_opt,table_id,column_id);
        if (unique_column)
        {
          sel = 1.0 / rel_opt->get_tuples();
        }
        else
        {
          ObBaseRelStatInfo * rel_stat_info = NULL;
          ObColumnStatInfo * col_stat_info = NULL;

          if (OB_SUCCESS == (ret = rel_opt->get_base_rel_stat_info(table_id,rel_stat_info))
              && rel_stat_info != NULL)
          {
            if (rel_stat_info->empty_table_)
            {
              sel = 1.0;
            }
            else if (OB_SUCCESS == (ret = rel_opt->get_column_stat_info(table_id,column_id,col_stat_info))
                     && col_stat_info != NULL)
            {
              common::hash::ObHashMap<oceanbase::common::ObObj, double, common::hash::NoPthreadDefendMode>::const_iterator iter =
                  col_stat_info->value_frequency_map_.begin();
              // because value type is not real type of the column ,so we only traversal all element
              // like decimal value change int
              common::ObExprObj v_map;
              for (; iter != col_stat_info->value_frequency_map_.end(); iter++)
              {
                v_map.assign(iter->first);

                if (v_map.get_type() == ObNullType )
                {
                  if (e_value.get_type() == ObNullType)
                  {
                    sel = iter->second;
                    //TBSYS_LOG(DEBUG, "QX eq sel = %.20lf ret = %d", sel, ret);
                    flag = true;
                    break;
                  }
                  continue;
                }
                ret = e_value.compare(v_map,cmp);
                // fix bug e_value is null
                if (ret == OB_RESULT_UNKNOWN)
                {
                  ret = OB_SUCCESS;
                }

                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(DEBUG,"QX WARN: compare fail =>%d",ret);
                  TBSYS_LOG(DEBUG,"QX value=%s e_value=%s mapv= %s v_map=%s cmp = %d",
                            to_cstring(value),to_cstring(e_value),to_cstring(iter->first),to_cstring(v_map),cmp);
                }
                else if (cmp == 0 )
                {
                  sel = iter->second;
                  //TBSYS_LOG(DEBUG, "QX eq sel = %.20lf ret = %d", sel, ret);
                  flag = true;
                  break;
                }
                else
                {
                  //TBSYS_LOG(DEBUG, "QX eq sel = %.20lf ret = %d cmp = %d", sel, ret, cmp);
                }
              }
              //not find eq value
              if (!flag)
              {
                sel = col_stat_info->avg_frequency_;
                TBSYS_LOG(DEBUG, "QX eq sel = %.20lf cid=%ld avg_freq %.20lf",
                          sel,col_stat_info->column_id_,col_stat_info->avg_frequency_);
              }
            }
            else
            {
              sel = DEFAULT_EQ_SEL;
              TBSYS_LOG(DEBUG, "QX no statistic of the table or column. table_id = %ld column_id = %ld unique_column =%d",table_id,column_id,unique_column);
            }
          }
          else
          {
            sel = DEFAULT_EQ_SEL;
            TBSYS_LOG(DEBUG, "QX no statistic of the table or column. table_id = %ld column_id = %ld unique_column =%d",table_id,column_id,unique_column);
          }

        }
        CLAMP_PROBABILITY(sel);
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }

      TBSYS_LOG(DEBUG, "QX eq sel = %.20lf", sel);

      return sel;
    }

    double ObStatSelCalculator::get_equal_subquery_selectivity(ObOptimizerRelation *rel_opt ,
                                                               ObSelInfo &sel_info)
    {
      double sel = 0.0;
      double high_frequency = 0.0;
      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;
      int ret = OB_SUCCESS;

      if (rel_opt->get_table_id() != table_id) //table don't own the expr
      {
        sel = 1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else
      {
        bool unique_column = is_unique_rowkey_column(rel_opt,table_id,column_id);
        if (unique_column)
        {
          sel = 1.0 / rel_opt->get_tuples();
        }
        else
        {
          ObColumnStatInfo* col_stat_info = NULL;
          if (OB_SUCCESS == (ret = rel_opt->get_column_stat_info(table_id,column_id,col_stat_info))
              && col_stat_info != NULL)
          {
            common::hash::ObHashMap<oceanbase::common::ObObj, double, common::hash::NoPthreadDefendMode>::const_iterator iter =
                col_stat_info->value_frequency_map_.begin();
            for (; iter != col_stat_info->value_frequency_map_.end(); iter++)
            {
              high_frequency += iter->second;
            }
            sel = 1.0 / col_stat_info->distinct_num_
                * high_frequency + (1.0 - high_frequency) * col_stat_info->avg_frequency_;
          }
          else
          {
            sel = DEFAULT_EQ_SEL;
          }
        }
        CLAMP_PROBABILITY(sel);
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }

      TBSYS_LOG(DEBUG, "QX eq subquery sel = %.20lf", sel);
      return sel;
    }

    double ObStatSelCalculator::get_lt_selectivity(ObOptimizerRelation *rel_opt ,
                                                   ObSelInfo &sel_info,
                                                   const oceanbase::common::ObObj &value)
    {
      double sel = 0.0;
      double  frequency1=0.0;
      double  frequency2=0.0;
      double  frequency3=0.0;
      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;
      int  ret = OB_SUCCESS;

      common::ObExprObj e_value;
      ObObj obj;
      char buf[OB_MAX_VARCHAR_LENGTH/32];
      cast_obj_to_exprObj(buf,obj,value,e_value);
      int cmp = 0;

      if (rel_opt->get_table_id() != table_id) //table don't own the expr
      {
        sel = 1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else
      {
        ObBaseRelStatInfo * rel_stat_info = NULL;
        ObColumnStatInfo * col_stat_info = NULL;

        if (OB_SUCCESS == (ret = rel_opt->get_base_rel_stat_info(table_id,rel_stat_info))
            && rel_stat_info != NULL)
        {
          if (rel_stat_info->empty_table_)
          {
            sel = 1.0;
          }
          else if (OB_SUCCESS == (ret = rel_opt->get_column_stat_info(table_id,column_id,col_stat_info))
                   && col_stat_info != NULL)
          {
            common::hash::ObHashMap<oceanbase::common::ObObj, double, common::hash::NoPthreadDefendMode>::const_iterator iter = col_stat_info->value_frequency_map_.begin();
            common::ObExprObj v_map;
            for (; iter != col_stat_info->value_frequency_map_.end(); iter++)
            {
              v_map.assign(iter->first);

              if (v_map.get_type() == ObNullType )
              {
                if (e_value.get_type() == ObNullType)
                {
                  frequency2 += iter->second;
                }
                else
                {
                  frequency3 += iter->second;
                }
                continue;
              }
              ret = e_value.compare(v_map,cmp);
              //TBSYS_LOG(INFO,"QX v=%s mapv= %s cmp = %d",print_obj(value),print_obj(iter->first),cmp);
              if (ret != OB_SUCCESS)
              {
                TBSYS_LOG(DEBUG,"QX WARN: compare fail =>%d",ret);
              }
              else if (cmp > 0)
              {
                frequency1 += iter->second;
              }
              else if (cmp == 0)
              {
                frequency2 += iter->second;
              }
              else
              {
                frequency3 += iter->second;
              }
            }
            sel += frequency1;
            sel +=  (1.0-frequency1-frequency2-frequency3) *
                common::calc_proportion(e_value, col_stat_info->max_value_, col_stat_info->min_value_);
            TBSYS_LOG(DEBUG, "QX lt sel = %.20lf frequency1 = %.20lf frequency2 = %.20lf frequency3 = %.20lf frequency sum = %.20lf proportion = %.20lf",
                      sel,frequency1,frequency2,frequency3,frequency1+frequency2+frequency3,
                      common::calc_proportion(e_value, col_stat_info->max_value_, col_stat_info->min_value_));
          }
          else
          {
            sel = DEFAULT_INEQ_SEL;
            TBSYS_LOG(DEBUG, "QX no statistic of the column. table_id = %ld column_id = %ld",table_id,column_id);
          }
        }
        else
        {
          sel = DEFAULT_INEQ_SEL;
          TBSYS_LOG(DEBUG, "QX no statistic of the table. table_id = %ld column_id = %ld",table_id,column_id);
        }
        CLAMP_PROBABILITY(sel);
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }
      TBSYS_LOG(DEBUG,"QX lt sel = %.20lf",sel);

      return sel;
    }

    double ObStatSelCalculator::get_le_selectivity(ObOptimizerRelation *rel_opt ,
                                                   ObSelInfo &sel_info,
                                                   const oceanbase::common::ObObj &value)
    {
      double sel = 0.0;
      double  frequency1=0.0;
      double  frequency2=0.0;
      double  frequency3=0.0;
      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;

      int  ret = OB_SUCCESS;
      //TBSYS_LOG(INFO,"QX value type = %d v=%s",value.get_type(),print_obj(value));

      //ObObj type value to ObExprObj type
      common::ObExprObj e_value;
      ObObj obj;
      char buf[OB_MAX_VARCHAR_LENGTH/32];
      cast_obj_to_exprObj(buf,obj,value,e_value);
      int cmp = 0;

      if (rel_opt->get_table_id() != table_id) //table don't own the expr
      {
        sel = 1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
        TBSYS_LOG(DEBUG,"QX table id not equal rel_opt's table_id.");
      }
      else
      {
        ObBaseRelStatInfo * rel_stat_info = NULL;
        ObColumnStatInfo * col_stat_info = NULL;

        if (OB_SUCCESS == (ret = rel_opt->get_base_rel_stat_info(table_id,rel_stat_info))
            && rel_stat_info != NULL)
        {
          if (rel_stat_info->empty_table_)
          {
            sel = 1.0;
          }
          else if (OB_SUCCESS == (ret = rel_opt->get_column_stat_info(table_id,column_id,col_stat_info))
                   && col_stat_info != NULL)
          {
            common::hash::ObHashMap<oceanbase::common::ObObj, double, common::hash::NoPthreadDefendMode>::const_iterator iter = col_stat_info->value_frequency_map_.begin();
            //the value in value_frequency_map_ to ObExprObj type
            common::ObExprObj v_map;
            for (; iter !=  col_stat_info->value_frequency_map_.end(); iter++)
            {
              //TBSYS_LOG(INFO,"QX cmp = %d v =%s  mapv =%s",cmp,print_obj(value),print_obj(iter->first));
              v_map.assign(iter->first);

              if (v_map.get_type() == ObNullType )
              {
                if (e_value.get_type() == ObNullType)
                {
                  frequency2 += iter->second;
                }
                else
                {
                   frequency3 += iter->second;
                }
                continue;
              }

              ret = e_value.compare(v_map,cmp);
              if (ret != OB_SUCCESS)
              {
                TBSYS_LOG(DEBUG,"QX WARN: compare fail =>%d",ret);
              }
              else if (cmp > 0)
              {
                frequency1 += iter->second;
              }
              else if (cmp == 0)
              {
                frequency2 += iter->second;
              }
              else
              {
                frequency3 += iter->second;
              }
            }
            sel += frequency1 + frequency2;
            sel +=  (1.0 - frequency1 - frequency2 - frequency3) *
                common::calc_proportion(e_value, col_stat_info->max_value_, col_stat_info->min_value_);
            TBSYS_LOG(DEBUG, "QX le sel = %.20lf frequency1 = %.20lf frequency2 = %.20lf frequency3 = %.20lf frequency sum = %.20lf proportion = %.20lf",
                      sel,frequency1,frequency2,frequency3,frequency1+frequency2+frequency3,
                      common::calc_proportion(e_value, col_stat_info->max_value_, col_stat_info->min_value_));
          }
          else
          {
            sel = DEFAULT_INEQ_SEL;
            TBSYS_LOG(DEBUG, "QX no statistic of the column. table_id = %ld column_id = %ld",table_id,column_id);
          }
        }
        else
        {
          sel = DEFAULT_INEQ_SEL;
          TBSYS_LOG(DEBUG, "QX no statistic of the table. table_id = %ld column_id = %ld",table_id,column_id);
        }
        CLAMP_PROBABILITY(sel);
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }
      TBSYS_LOG(DEBUG,"QX le sel = %.20lf",sel);

      return sel;
    }

    double ObStatSelCalculator::get_btw_selectivity(ObOptimizerRelation *rel_opt ,
                                                    ObSelInfo &sel_info,
                                                    const oceanbase::common::ObObj &value1,
                                                    const oceanbase::common::ObObj &value2)
    {
      double sel = 0.0;
      double  frequency1=0.0;
      double  frequency2=0.0;
      double  frequency3=0.0;
      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;
      int  ret = OB_SUCCESS;

      common::ObExprObj e_value1;
      common::ObExprObj e_value2;
      ObObj obj1;
      char buf1[OB_MAX_VARCHAR_LENGTH/32];
      cast_obj_to_exprObj(buf1,obj1,value1,e_value1);
      ObObj obj2;
      char buf2[OB_MAX_VARCHAR_LENGTH/32];
      cast_obj_to_exprObj(buf2,obj2,value2,e_value2);
      int cmp = 0;

      if (rel_opt->get_table_id() != table_id) //table don't own the expr
      {
        sel = 1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      //special handle
      else if (value1 > value2)
      {
        sel = 0.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = true;
      }
      else
      {
        ObBaseRelStatInfo * rel_stat_info = NULL;
        ObColumnStatInfo * col_stat_info = NULL;

        if (OB_SUCCESS == (ret = rel_opt->get_base_rel_stat_info(table_id,rel_stat_info))
            && rel_stat_info != NULL)
        {
          if (rel_stat_info->empty_table_)
          {
            sel = 1.0;
          }
          else if (OB_SUCCESS != (ret = rel_opt->get_column_stat_info(table_id,column_id,col_stat_info))
                   || col_stat_info == NULL)
          {
            sel = DEFAULT_RANGE_INEQ_SEL;
            TBSYS_LOG(DEBUG, "QX no statistic of the table. table_id = %ld column_id = %ld",table_id,column_id);
          }
          else if (common::is_numerical_type(value1) && common::is_numerical_type(value2))
          {
            common::hash::ObHashMap<oceanbase::common::ObObj, double, common::hash::NoPthreadDefendMode>::const_iterator iter = col_stat_info->value_frequency_map_.begin();
            common::ObExprObj v_map;
            for (; iter != col_stat_info->value_frequency_map_.end(); iter++)
            {
              print_obj(value1);
              print_obj(value2);
              print_obj(iter->first);

              v_map.assign(iter->first);
              if (v_map.get_type() == ObNullType )
              {
                if (e_value2.get_type() == ObNullType)
                {
                  frequency2 += iter->second;
                }
                else
                {
                   frequency3 += iter->second;
                }
                continue;
              }
              ret = e_value1.compare(v_map,cmp);
              if (ret != OB_SUCCESS)
              {
                TBSYS_LOG(DEBUG,"QX WARN: compare fail =>%d",ret);
              }
              else if (cmp > 0)
              {
                frequency1 += iter->second;
              }
              else if (cmp == 0)
              {
                frequency2 += iter->second;
              }
              else
              {
                e_value2.compare(v_map,cmp);
                if (cmp >= 0)
                {
                  frequency2 += iter->second;
                }
                else
                {
                  frequency3 += iter->second;
                }
              }
            }
            double max_proportion = common::calc_proportion(e_value2, col_stat_info->max_value_, col_stat_info->min_value_);
            double min_proportion = common::calc_proportion(e_value1, col_stat_info->max_value_, col_stat_info->min_value_);
            sel += frequency2;
            sel +=  (1.0-frequency1-frequency2-frequency3) *
                    ( max_proportion - min_proportion);
            TBSYS_LOG(DEBUG, "QX btw sel = %.20lf frequency1 = %.20lf frequency2 = %.20lf frequency3 = %.20lf frequency sum = %.20lf max_proportion = %.20lf min_proportion = %.20lf",
                      sel,frequency1,frequency2,frequency3,frequency1+frequency2+frequency3,
                      max_proportion,
                      min_proportion);
          }
          else
          {
            sel = DEFAULT_RANGE_INEQ_SEL;
            TBSYS_LOG(DEBUG,"QX value is not numerical type. table_id = %ld column_id = %ld",table_id,column_id);
          }
        }
        else
        {
          sel = DEFAULT_RANGE_INEQ_SEL;
          TBSYS_LOG(DEBUG, "QX no statistic of the column. table_id = %ld column_id = %ld",table_id,column_id);
        }
        CLAMP_PROBABILITY(sel);
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }
      TBSYS_LOG(DEBUG,"QX btw sel = %.20lf",sel);

      return sel;
    }

    #define FIXED_CHAR_SEL	0.20	/* about 1/5 */
    #define CHAR_RANGE_SEL	0.25
    #define ANY_CHAR_SEL	0.9		/* not 1, since it won't match end-of-string */
    #define FULL_WILDCARD_SEL 5.0
    #define PARTIAL_WILDCARD_SEL 2.0
    double ObStatSelCalculator::get_like_selectivity(ObOptimizerRelation *rel_opt ,
                                                     ObSelInfo &sel_info,
                                                     const oceanbase::common::ObObj &value)
    {
      double sel = 1.0;
      bool is_use_default_like_selectivity = true;

      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;

      UNUSED(value);
      UNUSED(column_id);
      //now have no the selectivity model of like
      if (rel_opt->get_table_id() != table_id) //table don't own the expr
      {
        sel = 1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else if (is_use_default_like_selectivity)
      {
        sel = DEFAULT_MATCH_SEL;
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }
      //it's not good effective seemingly, so we now use default sel.
      else if (value.get_type() == ObVarcharType)
      {
        // todo compute like sel
        //string pattern
        int pos,
            pattlen;
        char patt[OB_MAX_VARCHAR_LENGTH/32] = {0};
        ObString str;
        value.get_varchar(str);
        str.to_string(patt,str.length()+1);
        patt[str.length()]= '\0';
        pattlen = str.length();

        //fixed prefix
        for (pos = 0; pos < pattlen; pos++)
        {
          /* % and _ are wildcard characters in LIKE */
          if (patt[pos] == '%' ||
            patt[pos] == '_')
            break;

          /* Backslash escapes the next character */
          if (patt[pos] == '\\')
          {
            pos++;
            if (pos >= pattlen)
              break;
          }
        }
        // Skip any leading wildcard; it's already factored into initial sel
        for (; pos < pattlen; pos++)
        {
          if (patt[pos] != '%' && patt[pos] != '_')
            break;
        }

        for (; pos < pattlen; pos++)
        {
          /* % and _ are wildcard characters in LIKE */
          if (patt[pos] == '%')
            sel *= FULL_WILDCARD_SEL;
          else if (patt[pos] == '_')
            sel *= ANY_CHAR_SEL;
          else if (patt[pos] == '\\')
          {
            /* Backslash quotes the next character */
            pos++;
            if (pos >= pattlen)
              break;
            sel *= FIXED_CHAR_SEL;
          }
          else
            sel *= FIXED_CHAR_SEL;
        }

        TBSYS_LOG(DEBUG,"like '%s' sel = %.20lf",patt,sel);
      }
      else
      {
        sel = DEFAULT_MATCH_SEL;
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }

      CLAMP_PROBABILITY(sel);
      sel_info.enable = true;
      sel_info.selectivity_ = sel;
      TBSYS_LOG(DEBUG, "QX like sel = %.20lf", sel);
      return sel;
    }


    double ObStatSelCalculator::get_in_selectivity(ObOptimizerRelation *rel_opt,
                                                   ObSelInfo &sel_info,
                                                   const common::ObArray<ObObj> &value_array)
    {
      double sel = 0.0;
      uint64_t table_id = sel_info.table_id_;
      if (rel_opt->get_table_id() != table_id) //table don't own the expr
      {
        sel = 1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else
      {
        sel_info.enable = true;
        for (int64_t i = 0; i < value_array.count();i++ )
        {
          sel += get_equal_selectivity(rel_opt,sel_info,value_array.at(i));
        }
      }

      CLAMP_PROBABILITY(sel);
      sel_info.selectivity_ = sel;
      TBSYS_LOG(DEBUG, "QX in sel = %.20lf", sel);
      return sel;
    }

    /**
     * @brief ObStatCostCalculator::get_sort_cost
     * we don't separate in-memory sort and in-file sort
     * due to only open in-memory sort now but close in-file sort at oceanbase.
     * @return
     */
    int ObStatCostCalculator::get_sort_cost(double tuples, Cost & cost)
    {
      int ret = OB_SUCCESS;

      double comparison_cost = 0.0,run_cost = 0.0;
      comparison_cost += 2.0 * DEFAULT_CPU_OPERATOR_COST;
      if (tuples < 2)
      {
        tuples = 2;
      }
      cost += comparison_cost * tuples * LOG2(tuples);

      run_cost = tuples * DEFAULT_CPU_OPERATOR_COST;

      cost += run_cost;

      TBSYS_LOG(DEBUG,"QX tuples = %.20lf  sort cost = %.20lf",tuples,cost);

      return ret;
    }

    int ObStatCostCalculator::get_group_by_cost(ObSelectStmt *select_stmt,
                                                ObOptimizerRelation *rel_opt,
                                                ObIndexTableInfo *index_table_info,
                                                Cost & cost)
    {
      int ret = OB_SUCCESS;
      if (select_stmt== NULL || rel_opt == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX ERROR: select_stmt== NULL or rel_opt == NULL");
      }
      else
      {
        //index_table_info == NULL represent calculate seq cost
        //index_table_info != NULL represent calculate index cost
        if ((index_table_info == NULL
             && rel_opt->get_group_by_num() != 0
             && !rel_opt->get_seq_scan_info().group_by_applyed_)
            ||(rel_opt->get_group_by_num() !=0
               && index_table_info != NULL
               && !(index_table_info->group_by_applyed_)))
        {
          //sort cost
          get_sort_cost(rel_opt->get_rows(),cost);
          cost +=  rel_opt->get_rows() * DEFAULT_CPU_OPERATOR_COST * rel_opt->get_group_by_num();
        }
        else
        {
          // no need group by
        }
        TBSYS_LOG(DEBUG,"QX table_id =%ld group by cost = %.20lf",rel_opt->get_table_id(),cost);
      }
      return ret;
    }

    int ObStatCostCalculator::get_order_by_cost(ObSelectStmt *select_stmt,
                                                ObOptimizerRelation *rel_opt,
                                                ObIndexTableInfo *index_table_info,
                                                Cost & cost)
    {
      int ret = OB_SUCCESS;
      if (select_stmt== NULL || rel_opt == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX ERROR: select_stmt== NULL or rel_opt == NULL");
      }
      else
      {
        //index_table_info == NULL represent calculate seq cost
        //index_table_info != NULL represent calculate index cost
        if ((index_table_info == NULL
             && rel_opt->get_order_by_num()!=0
             && !rel_opt->get_seq_scan_info().order_by_applyed_)
            || (rel_opt->get_order_by_num() != 0
                && index_table_info != NULL
                && !(index_table_info->order_by_applyed_)))
        {
          //sort cost
          get_sort_cost(rel_opt->get_rows(),cost);
        }
        else
        {
          // no need order by
        }
        TBSYS_LOG(DEBUG,"QX table_id =%ld order by cost = %.20lf",rel_opt->get_table_id(),cost);
      }
      return ret;
    }

    int ObStatCostCalculator::get_cost_seq_scan(ObSelectStmt *select_stmt,
                                                ObOptimizerRelation *rel_opt,
                                                Cost & cost)
    {
      UNUSED(select_stmt);
      int ret = OB_SUCCESS;
      //bool unique_column =false;

      Cost total_cost = 0.0;
      Cost width = 0;
      //Cost avg_width= 0.0;

      Cost		cpu_run_cost = 0.0;
      Cost		disk_run_cost = 0.0;

      Cost    group_by_cost = 0.0;
      Cost    order_by_cost = 0.0;

      ObBaseRelStatInfo * rel_stat_info =NULL;
      //ObColumnStatInfo * col_stat_info = NULL;
      ObStatExtractor * stat_extractor =NULL;
      stat_extractor = rel_opt->get_stat_extractor();
      common::hash::ObHashMap<uint64_t, ObBaseRelStatInfo*, common::hash::NoPthreadDefendMode> * table_id_statInfo_map = NULL;
      table_id_statInfo_map = rel_opt->get_table_id_statInfo_map();

      if (table_id_statInfo_map == NULL || stat_extractor == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "QX rel_opt isn't initialization totally.");
      }
      else
      {
        ret = table_id_statInfo_map->get(rel_opt->get_table_id(), rel_stat_info);
        if (common::hash::HASH_EXIST == ret && rel_stat_info != NULL && rel_stat_info->enable_statinfo)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(DEBUG, "QX get statistic information of table success. table id = %ld", rel_opt->get_table_id());
        }
        else
        {
          ret = OB_ERROR;
          TBSYS_LOG(DEBUG, "QX WARN: get statistic information of table fail. table id = %ld", rel_opt->get_table_id());
        }
      }
      if (OB_SUCCESS != ret)
      {
        if (rel_stat_info != NULL)// enable_statinfo is false
        {
          total_cost = 1.0e10;  //disable cost
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(DEBUG,"QX WARN: rel_stat_info is NULL!");
        }
      }
      else
      {
        //compute width;
        //maybe we shouldn't commpute in here.
        // it maybe is not a good way but consider sstable well be compacted , we have no other way
        //avg_width = rel_stat_info->avg_width_ / rel_stat_info->column_num_;
        /*
        common::ObVector<uint64_t>::const_iterator iter= rel_opt->get_needed_columns().begin();
        for (;iter != rel_opt->get_needed_columns().end();iter++)
        {
          if (common::hash::HASH_EXIST != (ret = rel_stat_info->column_id_value_map_.get(*iter,col_stat_info)) ||
              col_stat_info == NULL)
          {
            // get statistic information of column
            ret = stat_extractor->fill_col_statinfo_map(table_id_statInfo_map,rel_opt,*iter,unique_column);
            if (ret != OB_SUCCESS)
            {
              //width += avg_width;
              //use schema info
              width += rel_stat_info->columns_width_[*iter];
            }
            else if (common::hash::HASH_EXIST != (ret = rel_stat_info->column_id_value_map_.get(*iter,col_stat_info)) ||
                     col_stat_info == NULL)
            {
              //use schema info
              width += rel_stat_info->columns_width_[*iter];
            }
            else
            {
              // use stat info, we not use it because columns_width is not correct.
              //use schema info
              width += rel_stat_info->columns_width_[*iter];
            }
          }
          else
          {
            //width += col_stat_info->avg_width_;
            //use schema info
            width += rel_stat_info->columns_width_[*iter];
          }
        }
        ret = OB_SUCCESS;
        */
        rel_opt->set_width(width);

        cpu_run_cost = rel_stat_info->tuples_ *
                       (DEFAULT_CPU_TUPLE_COST * 10 +  DEFAULT_CPU_OPERATOR_COST * static_cast<double>(rel_opt->get_base_cnd_list().size()));

        get_group_by_cost(select_stmt,rel_opt,NULL,group_by_cost);
        get_order_by_cost(select_stmt,rel_opt,NULL,order_by_cost);

        cpu_run_cost += group_by_cost + order_by_cost;
        cpu_run_cost *= DEFAULT_CPU_DISK_PROPORTION;

        disk_run_cost = rel_stat_info->size_ / (1<<13) * DEFAULT_SEQ_PAGE_COST;

        total_cost = cpu_run_cost + disk_run_cost;
      }
      TBSYS_LOG(DEBUG, "QX seq_scan group_by_cost = %.20lf", group_by_cost);
      TBSYS_LOG(DEBUG, "QX seq_scan order_by_cost = %.20lf", order_by_cost);
      TBSYS_LOG(DEBUG, "QX seq_scan cpu_run_cost = %.20lf", cpu_run_cost);
      TBSYS_LOG(DEBUG, "QX seq_scan disk_run_cost = %.20lf", disk_run_cost);
      TBSYS_LOG(DEBUG, "QX seq_scan table id =%ld ref_id =%ld total_cost = %.40lf", rel_opt->get_table_id(), rel_opt->get_table_ref_id(), total_cost);
      //add a start cost to make sure semi join right table is right
      total_cost += DEFAULT_START_COST;
      cost = total_cost;
      return ret;
    }

    int ObStatCostCalculator::get_cost_index_scan(ObSelectStmt *select_stmt,
                                                  ObOptimizerRelation *rel_opt,
                                                  ObIndexTableInfo &index_table_info,
                                                  const double sel)
    {
      int ret = OB_SUCCESS;
      //bool unique_column = false;

      Cost total_cost = 0.0;
      Cost cpu_run_cost = 0.0;
      Cost disk_run_cost = 0.0;

      Cost    group_by_cost = 0.0;
      Cost    order_by_cost = 0.0;

      uint64_t index_table_id = index_table_info.index_table_id_;
      //uint64_t index_column_id = index_table_info.index_column_id_;
      bool is_back_table = index_table_info.is_back_;

      uint64_t table_id = rel_opt->get_table_id();
      uint64_t table_ref_id = rel_opt->get_table_ref_id();

      //reuse rel_opt object
      rel_opt->set_table_id(index_table_id);
      rel_opt->set_table_ref_id(index_table_id);


      ObBaseRelStatInfo * rel_stat_info =NULL;
      ObStatExtractor * stat_extractor =NULL;
      stat_extractor = rel_opt->get_stat_extractor();
      common::hash::ObHashMap<uint64_t, ObBaseRelStatInfo*, common::hash::NoPthreadDefendMode> * table_id_statInfo_map = NULL;
      table_id_statInfo_map = rel_opt->get_table_id_statInfo_map();
      if (table_id_statInfo_map == NULL
          || stat_extractor == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG, "QX WARN: rel_opt isn't initialized totally.");
      }
      else
      {
        ret = table_id_statInfo_map->get(index_table_id, rel_stat_info);
        if (common::hash::HASH_EXIST == ret
            && rel_stat_info != NULL
            && rel_stat_info->enable_statinfo)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(DEBUG, "QX get statistic information of table success. table id = %ld", index_table_id);
        }
        else if (rel_stat_info != NULL
                 && !rel_stat_info->enable_statinfo)
        {
          ret = OB_ERROR;
          TBSYS_LOG(DEBUG, "QX WARN: get statistic information of table fail. table id = %ld", index_table_id);
        }
        //try query lastest statistic information of table
        else if (OB_SUCCESS != (ret = stat_extractor->fill_table_statinfo_map(table_id_statInfo_map,rel_opt)))
        {
          TBSYS_LOG(DEBUG, "QX WARN: no statistic information of table. table id = %ld",index_table_id);
        }
        else if (common::hash::HASH_EXIST == (ret = table_id_statInfo_map->get(index_table_id, rel_stat_info))&&
                 rel_stat_info != NULL)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(DEBUG, "QX get statistic information of table. table id = %ld", index_table_id);
        }
        else
        {
          ret = OB_ERROR;
          TBSYS_LOG(DEBUG, "QX WARN: get statistic information of index table fail. table id = %ld", index_table_id);
        }
      }

      //recovery rel_opt
      rel_opt->set_table_id(table_id);
      rel_opt->set_table_ref_id(table_ref_id);

      if (OB_SUCCESS != ret)
      {
        total_cost = 5.0e2;
        ret = OB_SUCCESS;
        TBSYS_LOG(DEBUG,"QX WARN: use default index cost.");
      }
      else
      {
        //sel is where condition  selectivity on index table
        cpu_run_cost = rel_stat_info->tuples_ * (DEFAULT_CPU_INDEX_TUPLE_COST * 10 + DEFAULT_CPU_OPERATOR_COST) * sel;

        get_group_by_cost(select_stmt,rel_opt,&index_table_info,group_by_cost);
        get_order_by_cost(select_stmt,rel_opt,&index_table_info,order_by_cost);

        cpu_run_cost += group_by_cost + order_by_cost;
        cpu_run_cost *= DEFAULT_CPU_DISK_PROPORTION;

        TBSYS_LOG(DEBUG, "QX index_scan group_by_cost = %.20lf", group_by_cost);
        TBSYS_LOG(DEBUG, "QX index_scan order_by_cost = %.20lf", order_by_cost);

        TBSYS_LOG(DEBUG, "QX index_scan index table id = %ld, index table cpu_run_cost = %.40lf",index_table_id, cpu_run_cost);
        // we think sel and cost is positive relation, distinct_num_ and cost is negative relation
        disk_run_cost = rel_stat_info->size_ / (1<<13) * DEFAULT_SEQ_PAGE_COST * sel;// / col_stat_info->distinct_num_

        if (is_back_table)
        {
          double tmp_cnd_size = 0.0;
          tmp_cnd_size = static_cast<double>(rel_opt->get_base_cnd_list().size() - 1);
          //get origin table statinfo
          if (common::hash::HASH_EXIST != (ret = table_id_statInfo_map->get(rel_opt->get_table_id(),rel_stat_info))||
              rel_stat_info == NULL)
          {
            ret = OB_SUCCESS;
            cpu_run_cost += 1.0e1;
            disk_run_cost += 2.0e2;
            TBSYS_LOG(DEBUG, "QX WARN: use default index disk and cpu cost,disk_run_cost = %.40lf", disk_run_cost);
          }
          else
          {
            ret = OB_SUCCESS;
            Cost tmp_cpu_cost = cpu_run_cost;
            cpu_run_cost = rel_stat_info->tuples_ * sel * ( DEFAULT_CPU_TUPLE_COST * 10 + tmp_cnd_size * DEFAULT_CPU_OPERATOR_COST);
            cpu_run_cost *= DEFAULT_CPU_DISK_PROPORTION;
            cpu_run_cost += tmp_cpu_cost;
            TBSYS_LOG(DEBUG, "QX index_scan back src table cpu_run_cost = %.40lf", cpu_run_cost);
            //DEFAULT_RANDOM_PAGE_COST is 4 * DEFAULT_SEQ_PAGE_COST is means that
            //sel must less 0.25 then index scan eable cheaper than seq scan
            disk_run_cost += rel_stat_info->size_ /(1<<13) * DEFAULT_RANDOM_PAGE_COST * sel;
            TBSYS_LOG(DEBUG, "QX index_scan disk_run_cost = %.40lf", disk_run_cost);
          }
        }
        total_cost = cpu_run_cost + disk_run_cost;
      }

      TBSYS_LOG(DEBUG, "QX index_scan table id =%ld index_table_id =%ld total_cost = %.40lf",rel_opt->get_table_id(),index_table_id, total_cost);
      total_cost += DEFAULT_START_COST;
      index_table_info.cost_ = total_cost;

      return ret;
    }

    int ObStatExtractor::fill_col_statinfo_map(common::hash::ObHashMap<uint64_t, ObBaseRelStatInfo*, common::hash::NoPthreadDefendMode> * table_id_statInfo_map,
                                               ObOptimizerRelation *rel_opt,
                                               uint64_t column_id,
                                               bool unique_column_rowkey)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = rel_opt->get_table_id();
      uint64_t table_ref_id = rel_opt->get_table_ref_id();
      oceanbase::mergeserver::StatisticColumnValue scv;
      sql::ObBaseRelStatInfo * rel_stat_info = NULL;
      ObColumnStatInfo * col_stat_info = NULL;

      common::ObObj obj_min;
      common::ObObj obj_max;

      bool empty_table = false;

      if (table_id_statInfo_map ==NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX WARN: table_id_statInfo_map is NULL.");
      }
      else if (statistic_info_cache_== NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX ERROR: statistic_info_cache is NULL.");
      }
      // get rel_stat_info
      else if(common::hash::HASH_EXIST != (ret = table_id_statInfo_map->get(table_id,rel_stat_info))
              || rel_stat_info == NULL
              || !rel_stat_info->enable_statinfo)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX WARN: get rel_stat_info is NULL!");
      }
      else // predict to avoid invalid query
      {
        ret = OB_ERROR;
        for (int i =0; i < rel_stat_info->statistic_columns_num_ ;i++)
        {
          if (rel_stat_info->statistic_columns_[i] == column_id)
          {
            ret = OB_SUCCESS;
            break;
          }
        }
      }

      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(DEBUG,"QX WARN: no cloumn statistics information.");
      }
      else if (OB_SUCCESS !=(ret = statistic_info_cache_->get_column_statistic_info_from_cache(table_ref_id,column_id,scv)))
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX WARN: get_tv_from_map is fail.");
      }
      //push into map
      else
      {
        if (common::hash::HASH_EXIST != rel_stat_info->column_id_value_map_.get(column_id,col_stat_info) ||
            col_stat_info == NULL)
        {
          //col_stat_info = OB_NEW(ObColumnStatInfo,ObModIds::OB_QUERY_OPT);
          void * buf = NULL;
          if (rel_opt->get_name_pool() == NULL)
          {
            TBSYS_LOG(ERROR,"QX rel_opt name_pool is null.");
            ret = OB_ERROR;
          }
          else if ((buf = rel_opt->get_name_pool()->alloc(sizeof(ObColumnStatInfo))) == NULL)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(ERROR,"QX col_stat_info malloc memory fail!");
          }
          else
          {
            col_stat_info = new (buf) ObColumnStatInfo();
          }
        }
        if (col_stat_info == NULL)
        {
        }
        else if (OB_SUCCESS != (ret = scv.min_.obj_copy(obj_min)))
        {
          TBSYS_LOG(ERROR,"QX min value object copy fail!");
        }
        else if (OB_SUCCESS != (ret = scv.max_.obj_copy(obj_max)))
        {
          TBSYS_LOG(ERROR,"QX max value object copy fail!");
        }
        else
        {
          col_stat_info->column_id_ = column_id;
          TBSYS_LOG(DEBUG,"QX column_id_ = %ld ",col_stat_info->column_id_ );
          if (scv.different_num_ < 1)
          {
            empty_table = true;
          }
          col_stat_info->distinct_num_ = static_cast<double>(scv.different_num_);
          if (empty_table)
          {
             col_stat_info->distinct_num_ = 1.0;
          }
          TBSYS_LOG(DEBUG,"QX distinct_num_ = %.20lf ",col_stat_info->distinct_num_ );
          if (!col_stat_info->value_frequency_map_.created())
          {
            col_stat_info->value_frequency_map_.create(STAT_INFO_FREQ_MAP_SIZE);
          }
          //min
          col_stat_info->min_value_.assign(obj_min);
          TBSYS_LOG(DEBUG,"QX min = %s",to_cstring(obj_min));
          //max
          col_stat_info->max_value_.assign(obj_max);
          TBSYS_LOG(DEBUG,"QX max = %s",to_cstring(obj_max));
          // width ?
          if (scv.row_count_ == 0 || rel_stat_info->column_num_ == 0) // avoid div 0
          {
            col_stat_info->avg_width_ = WIDTH_ARRY[scv.top_value_[0].obj_.get_type()];
          }
          else
          {
            col_stat_info->avg_width_  = static_cast<double>(scv.size_) / static_cast<double>(scv.row_count_) / rel_stat_info->column_num_;
          }
          TBSYS_LOG(DEBUG,"QX avg_width_ = %.20lf ",col_stat_info->avg_width_);
          // unique column in primary key?
          col_stat_info->unique_rowkey_column_ = unique_column_rowkey;
          TBSYS_LOG(DEBUG,"QX unique_rowkey_column_ = %d ",col_stat_info->unique_rowkey_column_);

          if (ObOPtimizerLoger::log_switch_)
          {
            char tmp[256]={0};
            snprintf(tmp,256,"QOQX fill_col_statinfo_map start table_ref_id=%ld >>>>> ",rel_opt->get_table_ref_id());
            ObOPtimizerLoger::print(tmp);
            snprintf(tmp,256,"QOQX table_id =%ld column_id_ = %ld",rel_opt->get_table_id(),col_stat_info->column_id_);
            ObOPtimizerLoger::print(tmp);
            snprintf(tmp,256,"QOQX min = %s",to_cstring(col_stat_info->min_value_));
            ObOPtimizerLoger::print(tmp);
            snprintf(tmp,256,"QOQX max = %s",to_cstring(col_stat_info->max_value_));
            ObOPtimizerLoger::print(tmp);
            snprintf(tmp,256,"QOQX unique_rowkey_column_ = %d",col_stat_info->unique_rowkey_column_);
            ObOPtimizerLoger::print(tmp);
          }

          // need consider less than 10
          double total_high_frequency = 0, freq= 0;
          for (int i = 0; ret == OB_SUCCESS && i < scv.top_value_num_ ; i++)
          {
            freq = static_cast<double>(scv.top_value_[i].num_)/static_cast<double>(scv.row_count_);
            //ObObj & obj = scv.top_value_[i].obj_;
            //scv.top_value_[i].obj_.obj_copy(col_stat_info->obj[i]);
            if(OB_SUCCESS != (ret = rel_opt->copy_obj(scv.top_value_[i].obj_,col_stat_info->obj[i])))
            {
              TBSYS_LOG(WARN,"QX write object fail. ret = %d",ret);
            }
            else
            {
              ret = col_stat_info->value_frequency_map_.set(col_stat_info->obj[i], freq, 1);
            }
            total_high_frequency += freq;
            TBSYS_LOG(DEBUG,"QX value_frequency_map i[%d]k:%s v:%.20lf  col_stat_info->value_frequency_map_ size = %ld ret = %d",
                      i,to_cstring(col_stat_info->obj[i]),freq,col_stat_info->value_frequency_map_.size(),ret);

            if (ObOPtimizerLoger::log_switch_)
            {
              char tmp[256]={0};
              snprintf(tmp,256,"QOQX value_frequency_map[%d] k: %s v: %.6lf size = %ld",i,to_cstring(col_stat_info->obj[i]),freq,col_stat_info->value_frequency_map_.size());
              ObOPtimizerLoger::print(tmp);
            }

            if (ret == common::hash::HASH_OVERWRITE_SUCC || ret == common::hash::HASH_INSERT_SUCC)
            {
              ret = OB_SUCCESS;
            }
            else
            {
              TBSYS_LOG(DEBUG,"QX WARN:value_frequency_map_ set value fail,No.%d,obj=%s.",i,to_cstring(col_stat_info->obj[i]));
            }
          }
          // avg_frequency
          if (scv.top_value_num_ >= scv.different_num_ ||
              (1.0 - total_high_frequency) <= 0)
          {
            col_stat_info->avg_frequency_ = 0;
          }
          else
          {
            col_stat_info->avg_frequency_ = (1.0 - total_high_frequency) *  1.0 / static_cast<double>(scv.different_num_ - scv.top_value_num_);
          }
          TBSYS_LOG(DEBUG,"QX avg_frequency_ = %.20lf ",col_stat_info->avg_frequency_ );
          if (ObOPtimizerLoger::log_switch_)
          {
            char tmp[256]={0};
            snprintf(tmp,256,"QOQX avg_frequency_ = %.6lf",col_stat_info->avg_frequency_);
            ObOPtimizerLoger::print(tmp);
            snprintf(tmp,256,"QOQX fill_col_statinfo_map end <<<<<<");
            ObOPtimizerLoger::print(tmp);
          }

        }
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG,"QX WARN: value_frequency_map_ add fail!");
        }
        else if (rel_stat_info->column_id_value_map_.created())
        {
        }
        else if (OB_SUCCESS != (ret = rel_stat_info->column_id_value_map_.create(STAT_INFO_COL_MAP_SIZE)))
        {
          TBSYS_LOG(ERROR,"QX rel_stat_info->column_id_value_map_ create fail =>%d",ret);
        }

        if (OB_SUCCESS != ret)
        {
        }
        else if (common::hash::HASH_OVERWRITE_SUCC == (ret = rel_stat_info->column_id_value_map_.set(column_id,col_stat_info,1))
                 || common::hash::HASH_INSERT_SUCC == ret)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(DEBUG,"QX column_id_value_map_ add success! column_id_value_map_.size = %ld =>%d",rel_stat_info->column_id_value_map_.size(),ret);
        }
        else
        {
          TBSYS_LOG(DEBUG,"QX ERROR: column_id_value_map_ add fail! column_id_value_map_.size = %ld =>%d",rel_stat_info->column_id_value_map_.size(),ret);
        }
      }
      //add huangcc [prevent mermory leak]20170714:b
      scv.allocator_.free();
      //add:e
      return ret;
    }

    int ObStatExtractor::fill_table_statinfo_map(common::hash::ObHashMap<uint64_t, ObBaseRelStatInfo*, common::hash::NoPthreadDefendMode> * table_id_statInfo_map,
                                                 ObOptimizerRelation *rel_opt)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = rel_opt->get_table_id();
      uint64_t table_ref_id = rel_opt->get_table_ref_id();
      sql::ObBaseRelStatInfo * rel_stat_info = NULL;
      oceanbase::mergeserver::StatisticTableValue stv;

      bool empty_table = false;

      if (!table_id_statInfo_map->created())
      {
        ret = table_id_statInfo_map->create(STAT_INFO_TABLE_MAP_SIZE);
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(DEBUG,"QX ERROR: table_id_statInfo_map create fail.=>%d",ret);
      }
      else if (common::hash::HASH_EXIST != table_id_statInfo_map->get(table_id,rel_stat_info)||
               rel_stat_info == NULL)
      {
        void * buf = NULL;
        if (rel_opt->get_name_pool() == NULL)
        {
          TBSYS_LOG(ERROR,"rel_opt name_pool is null.");
          ret = OB_ERROR;
        }
        else if ((buf = rel_opt->get_name_pool()->alloc(sizeof(ObBaseRelStatInfo))) == NULL)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR,"QX rel_stat_info malloc memory fail!");
        }
        else
        {
          rel_stat_info = new (buf) ObBaseRelStatInfo();
        }
      }

      if (rel_stat_info == NULL)
      {
      }
      else if (statistic_info_cache_ == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX ERROR: statistic_info_cache is NULL.");
      }
      // get statitics
      else if (OB_SUCCESS == (ret = statistic_info_cache_->get_table_statistic_info_from_cache(table_ref_id,stv)))
      {
        // fill table information
        rel_stat_info->table_id_ = table_id;
        TBSYS_LOG(DEBUG,"QX table_id = %ld ref_id = %ld",table_id,table_ref_id);

        if (stv.statistic_columns_num_<=0)
        {
          rel_stat_info->enable_statinfo=false;
        }
        else
        // at least row count >= 1
        if (stv.row_count_ < 1)
        {
          empty_table = true;
          rel_stat_info->tuples_ = 1.0;
        }
        else
        {
          empty_table = false;
          rel_stat_info->tuples_ = static_cast<double>(stv.row_count_);
        }
        TBSYS_LOG(DEBUG,"QX tuples_ = %.20lf",rel_stat_info->tuples_);
        rel_stat_info->avg_width_ = static_cast<double>(stv.mean_row_size_);
        TBSYS_LOG(DEBUG,"QX avg_width_ = %.20lf",rel_stat_info->avg_width_);
        rel_stat_info->statistic_columns_num_ = stv.statistic_columns_num_;
        TBSYS_LOG(DEBUG,"QX statistic_columns_num_ = %ld",rel_stat_info->statistic_columns_num_);
        rel_stat_info->size_ = static_cast<double>(stv.size_);
        TBSYS_LOG(DEBUG,"QX size_ = %lf",rel_stat_info->size_);
        rel_stat_info->empty_table_ = empty_table;
        TBSYS_LOG(DEBUG,"QX empty_table_ = %d",rel_stat_info->empty_table_);
        TBSYS_LOG(DEBUG,"QX enable_statinfo = %d",rel_stat_info->enable_statinfo);

        if (ObOPtimizerLoger::log_switch_)
        {
          char tmp[256]={0};
          snprintf(tmp,256,"QOQX fill_table_statinfo_map start >>>>>");
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX table_id = %ld table_ref_id = %ld",table_id,table_ref_id);
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX tuples_ = %.6lf",rel_stat_info->tuples_);
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX statistic_columns_num_ = %ld",rel_stat_info->statistic_columns_num_);
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX size_ = %.6lf",rel_stat_info->size_);
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX empty_table_ = %d",rel_stat_info->empty_table_);
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX fill_table_statinfo_map end <<<<<");
          ObOPtimizerLoger::print(tmp);
        }

        for (int i=0;i<stv.statistic_columns_num_;i++)
        {
          rel_stat_info->statistic_columns_[i] = stv.statistic_columns_[i];
          TBSYS_LOG(DEBUG,"QX column statistics info i =%d column id = %ld",i,rel_stat_info->statistic_columns_[i]);
        }
        // warn: maybe need free memory
        if (!rel_stat_info->column_id_value_map_.created())
        {
          rel_stat_info->column_id_value_map_.create(STAT_INFO_COL_MAP_SIZE);
        }
      }
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(DEBUG,"QX no statitics return from cache, table_id = %ld",table_id);
      }
      else if (common::hash::HASH_OVERWRITE_SUCC == (ret = table_id_statInfo_map->set(table_id,rel_stat_info,1)) ||
               common::hash::HASH_INSERT_SUCC == ret)
      {
        ret = OB_SUCCESS;
        //rel_stat_info->enable_statinfo = true;
        TBSYS_LOG(DEBUG,"QX table_id_statInfo_map_ add success! table_id_statInfo_map->size() =%ld => %d",
                  table_id_statInfo_map->size(),ret);
      }
      else
      {
        TBSYS_LOG(DEBUG,"QX WARN: table_id_statInfo_map_ add fail! table_id_statInfo_map->size() =%ld  => %d",
                  table_id_statInfo_map->size(),ret);
      }
      return ret;
    }

    int ObOptimizerRelation::get_column_stat_info(uint64_t table_id,
                                                  uint64_t column_id,
                                                  ObColumnStatInfo* &col_stat_info)
    {
      int ret = OB_SUCCESS;
      ObBaseRelStatInfo* rel_stat_info = NULL;
      bool unique_column = false;
      unique_column = ObStatSelCalculator::is_unique_rowkey_column(this,table_id,column_id);
      if (OB_SUCCESS != (ret = get_base_rel_stat_info(table_id,rel_stat_info)))
      {
        TBSYS_LOG(DEBUG,"QX get relation stat info fail ret = %d, table_id =%ld",ret,table_id);
      }
      //get column info
      else if (rel_stat_info->enable_statinfo
               && common::hash::HASH_EXIST == (ret = rel_stat_info->column_id_value_map_.get(column_id,col_stat_info))
               && col_stat_info != NULL)
      {
        ret = OB_SUCCESS;
        TBSYS_LOG(DEBUG,"QX get column stat info table_id %ld column_id = %ld",table_id,column_id);
      }
      else if (rel_stat_info->enable_statinfo)
      {
        // try query lastest statistic information of column
        ret = stat_extractor_->fill_col_statinfo_map(table_id_statInfo_map_,this,column_id,unique_column);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG,"QX can't get column stat info table_id %ld column_id = %ld",table_id,column_id);
        }
        else if (common::hash::HASH_EXIST == (ret = rel_stat_info->column_id_value_map_.get(column_id,col_stat_info))
                 && col_stat_info != NULL)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(DEBUG,"QX get column stat info table_id %ld column_id = %ld",table_id,column_id);
        }
        else
        {
          TBSYS_LOG(DEBUG,"QX can't get column stat info table_id %ld column_id = %ld =>%d",table_id,column_id,ret);
        }
      }
      return ret;
    }

    int ObOptimizerRelation::get_base_rel_stat_info(uint64_t table_id,
                                                    ObBaseRelStatInfo* & rel_stat_info)
    {
      int ret = OB_SUCCESS;
      oceanbase::mergeserver::ObStatisticInfoCache *statistic_info_cache = NULL;
      if (schema_managerv2_ == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX ERROR: schema_managerv2_ is null");
      }
      else if (stat_extractor_ == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX ERROR: stat_extractor_ is null");
      }
      else
      {
        statistic_info_cache = stat_extractor_->get_statistic_info_cache();
      }

      if (table_id_statInfo_map_ == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX ERROR: table_id_statInfo_map_ is NULL.");
      }
      else if (statistic_info_cache == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX ERROR: statistic_info_cache is NULL.");
      }
      // get rel_stat_info
      else if(common::hash::HASH_EXIST != (ret = table_id_statInfo_map_->get(table_id,rel_stat_info))||
              rel_stat_info == NULL||
              !rel_stat_info->enable_statinfo)
      {
        ret = OB_ERROR;
        TBSYS_LOG(DEBUG,"QX WARN: get rel_stat_info fail, table_id = %ld",table_id);
      }
      else
      {
        ret = OB_SUCCESS;
      }

      return ret;
    }

    void ObOptimizerRelation::reset_semi_join_right_index_table_cost(uint64_t column_id,
                                                                     double sel)
    {
      int32_t cheapest_index_table_idx = 0;
      double tmp_cheapest_cost = DBL_MAX;
      bool flag = false;
      if (index_table_array_.size() > 0)
      {
        for (int32_t i = 0 ; i < index_table_array_.size();i++)
        {
          if (index_table_array_.at(i).index_column_id_ == column_id && index_table_array_.at(i).cost_ < tmp_cheapest_cost)
          {
            tmp_cheapest_cost = index_table_array_.at(i).cost_;
            cheapest_index_table_idx = i;
            flag = true;
          }
        }
        if (flag)
        {
          index_table_array_.at(cheapest_index_table_idx).cost_ = tmp_cheapest_cost * sel;
          TBSYS_LOG(DEBUG,"QX reset cost: table id = %ld ref_id id =%ld index table id = %ld column_id = %ld, cost=%.20lf",table_id_,
                    table_ref_id_,
                    index_table_array_.at(cheapest_index_table_idx).index_table_id_,
                    index_table_array_.at(cheapest_index_table_idx).index_column_id_,
                    index_table_array_.at(cheapest_index_table_idx).cost_);
        }
        else
        {
          //fix bug : semi join Invalid argument
          //semi join use origin table however base table use index table
          //so we need clear index_table_array_
          index_table_array_.clear();

          TBSYS_LOG(DEBUG,"QX reset cost: table id = %ld ref_id id =%ld column_id = %ld not find right index table.",table_id_,table_ref_id_,column_id);
        }
      }
      else
      {
        TBSYS_LOG(DEBUG,"QX reset cost: table id = %ld ref_id id =%ld index_table_array_ count == 0",table_id_,table_ref_id_);
      }
    }

    void ObOptimizerRelation::print_rel_opt_info()
    {
      TBSYS_LOG(DEBUG,"QX ====================rel_opt_info=begin========================");
      TBSYS_LOG(DEBUG,"QX table_id = %ld",table_id_);
      TBSYS_LOG(DEBUG,"QX table_ref_id_ = %ld",table_ref_id_);
      TBSYS_LOG(DEBUG,"QX rel_opt_kind_ = %d",rel_opt_kind_);
      TBSYS_LOG(DEBUG,"QX selectivity_i = %.20lf",rows_ / tuples_);
      TBSYS_LOG(DEBUG,"QX rows_ = %.20lf",rows_);
      TBSYS_LOG(DEBUG,"QX tuples_ = %.20lf",tuples_);
      TBSYS_LOG(DEBUG,"QX join_rows_ = %.20lf",join_rows_);
      TBSYS_LOG(DEBUG,"QX selectivity_j = %.20lf",join_rows_ / tuples_);
      TBSYS_LOG(DEBUG,"QX group_by_num_ = %d",group_by_num_);
      TBSYS_LOG(DEBUG,"QX order_by_num_ = %d",order_by_num_);
      TBSYS_LOG(DEBUG,"QX seq_scan_cost_ = %.20lf",get_seq_scan_cost());
      for (int32_t i = 0; i < index_table_array_.size();i++)
      {
        TBSYS_LOG(DEBUG,"QX NO.%d,index_table_id=%ld,column_id=%ld,is_back=%d,cost=%.20lf,group_by_applyed_=%d,order_by_applyed_=%d",
                  i,index_table_array_.at(i).index_table_id_,
                  index_table_array_.at(i).index_column_id_,
                  index_table_array_.at(i).is_back_,
                  index_table_array_.at(i).cost_,
                  index_table_array_.at(i).group_by_applyed_,
                  index_table_array_.at(i).order_by_applyed_);
      }
      TBSYS_LOG(DEBUG,"QX base_cnd_list_ size = %d",base_cnd_list_.size());
      TBSYS_LOG(DEBUG,"QX join_cnd_list_ size =%d",join_cnd_list_.size());
      //TBSYS_LOG(DEBUG,"QX needed_columns_ size = %d",needed_columns_.size());
      TBSYS_LOG(DEBUG,"QX ====================rel_opt_info=end========================");
    }

    void ObOptimizerRelation::print_rel_opt_info(const FILE* file)
    {
      if (ObOPtimizerLoger::log_switch_)
      {
        FILE* fp = const_cast<FILE*>(file);
        fprintf(fp,"====================rel_opt_info=begin========================\n");
        fprintf(fp,"\ttable_id = %ld\n",table_id_);
        fprintf(fp,"\ttable_ref_id_ = %ld\n",table_ref_id_);
        fprintf(fp,"\trel_opt_kind_ = %d\n",rel_opt_kind_);
        fprintf(fp,"\tselectivity_i = %.20lf\n",rows_ / tuples_);
        fprintf(fp,"\trows_ = %.20lf\n",rows_);
        fprintf(fp,"\ttuples_ = %.20lf\n",tuples_);
        fprintf(fp,"\tjoin_rows_ = %.20lf\n",join_rows_);
        fprintf(fp,"\tselectivity_j = %.20lf\n",join_rows_ / tuples_);
        fprintf(fp,"\tgroup_by_num_ = %d\n",group_by_num_);
        fprintf(fp,"\torder_by_num_ = %d\n",order_by_num_);
        fprintf(fp,"\tseq_scan_cost_ = %.20lf\n",get_seq_scan_cost());
        for (int32_t i = 0; i < index_table_array_.size();i++)
        {
           fprintf(fp,"\tNO.%d,index_table_id =%ld, column_id =%ld, is_back=%d,cost=%.20lf,group_by_applyed_=%d,order_by_applyed_=%d\n",
                    i,index_table_array_.at(i).index_table_id_,
                    index_table_array_.at(i).index_column_id_,
                    index_table_array_.at(i).is_back_,
                    index_table_array_.at(i).cost_,
                   index_table_array_.at(i).group_by_applyed_,
                   index_table_array_.at(i).order_by_applyed_);
        }
        fprintf(fp,"\tbase_cnd_list_ size = %d\n",base_cnd_list_.size());
        fprintf(fp,"\tjoin_cnd_list_ size =%d\n",join_cnd_list_.size());
        fprintf(fp,"\tneeded_columns_ size = %d\n",needed_columns_.size());
        fprintf(fp,"====================rel_opt_info=end==========================\n");
      }
    }

    void ObOptimizerRelation::print_rel_opt_info_V2()
    {
      if (ObOPtimizerLoger::log_switch_)
      {
        char tmp[256]={0};
        snprintf(tmp,256,"QOQX ===========rel_opt_info=begin===========");
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX table_id = %ld",table_id_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX table_ref_id_ = %ld",table_ref_id_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX rel_opt_kind_ = %d",rel_opt_kind_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX selectivity_i = %.6lf",rows_ / tuples_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX rows_ = %.6lf",rows_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX tuples_ = %.6lf",tuples_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX join_rows_ = %.6lf",join_rows_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX selectivity_j = %.6lf",join_rows_ / tuples_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX group_by_num_ = %d",group_by_num_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX order_by_num_ = %d",order_by_num_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX seq_scan_cost_ = %.6lf",get_seq_scan_cost());
        ObOPtimizerLoger::print(tmp);
        for (int32_t i = 0; i < index_table_array_.size();i++)
        {
          snprintf(tmp,256,"QOQX NO.%d,index_table_id=%ld,column_id=%ld,is_back=%d,cost=%.6lf,group_by_applyed_=%d,order_by_applyed_=%d"
                   ,i,index_table_array_.at(i).index_table_id_
                   ,index_table_array_.at(i).index_column_id_
                   ,index_table_array_.at(i).is_back_
                   ,index_table_array_.at(i).cost_,
                   index_table_array_.at(i).group_by_applyed_,
                   index_table_array_.at(i).order_by_applyed_);
          ObOPtimizerLoger::print(tmp);
        }
        snprintf(tmp,256,"QOQX base_cnd_list_ size = %d",base_cnd_list_.size());
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX join_cnd_list_ size = %d",join_cnd_list_.size());
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"QOQX ============rel_opt_info=end============");
        ObOPtimizerLoger::print(tmp);
      }
    }

  }
}

