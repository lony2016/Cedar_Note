/*
 *
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * ob_merge_callback.h is for what ...
 *
 * Version: ***: ob_merge_callback.h  Fri May 25 13:37:11 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want 
 *
 */
#ifndef OB_MERGE_CALLBACK_H_
#define OB_MERGE_CALLBACK_H_

#include "common/ob_packet_factory.h"
<<<<<<< HEAD
#include "onev_struct.h"
=======
#include "easy_io_struct.h"
>>>>>>> refs/remotes/origin/master

namespace oceanbase
{
  namespace mergeserver
  {
    using namespace oceanbase::common;
    class ObMergeCallback
    {
    public:

      /**
       * callback func of main service
       * @param req   requset to process
<<<<<<< HEAD
       * @return int  return ONEV_AGAIN if push it into queue
       *              else return ONEV_ERROR
       */
      static int process(onev_request_e* req);
=======
       * @return int  return EASY_AGAIN if push it into queue
       *              else return EASY_ERROR
       */
      static int process(easy_request_t* req);
>>>>>>> refs/remotes/origin/master

      /**
       * callback func of async rpc call
       * called when receive async rpc call response
       * @param req   request to process
<<<<<<< HEAD
       * @return int  return ONEV_OK if request processed successful
       *              else return ONEV_ERROR
       */
      static int rpc_process(onev_request_e* req);
=======
       * @return int  return EASY_OK if request processed successful
       *              else return EASY_ERROR
       */
      static int rpc_process(easy_request_t* req);
>>>>>>> refs/remotes/origin/master
      
      /**
       * callback func of sql async  rpc call
       * called when receive async rpc call response
       * @param req   request to process
<<<<<<< HEAD
       * @return int  return ONEV_OK if request processed successful
       *              else return ONEV_ERROR
       */
      static int sql_process(onev_request_e* req);
=======
       * @return int  return EASY_OK if request processed successful
       *              else return EASY_ERROR
       */
      static int sql_process(easy_request_t* req);
>>>>>>> refs/remotes/origin/master

      static ObPacketFactory packet_factory_;
    };
  }
}
#endif
