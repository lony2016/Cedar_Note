#include "ob_mysql_callback.h"
#include "common/ob_define.h"
#include "tblog.h"
#include "ob_mysql_server.h"
#include "ob_mysql_util.h"
#include "obmysql/ob_mysql_server.h"
#include "common/utility.h"
#include "common/hash/ob_hashutils.h"
#include "obmysql/packet/ob_mysql_error_packet.h"
#include "common/base_main.h"
#include <sys/uio.h>
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
namespace oceanbase
{
  namespace obmysql
  {
    int ObMySQLCallback::encode(onev_request_e* r, void* packet)
    {
      int ret = ONEV_OK;
      if (NULL == r || NULL == packet)
      {
        TBSYS_LOG(ERROR, "invalid argument r=%p, packet=%p", r, packet);
        ret = ONEV_ERROR;
      }
      else
      {
        onev_buf_e* buf = reinterpret_cast<onev_buf_e*>(packet);
        onev_request_addbuf(r, buf);
      }
      return ret;
    }

    void* ObMySQLCallback::decode(onev_message_e* m)
    {
      uint32_t pkt_len = 0;
      uint8_t pkt_seq = 0;
      uint8_t pkt_type = 0;
      ObMySQLCommandPacket* packet = NULL;
      char* buffer = NULL;
      int32_t len = 0;

      if (NULL == m)
      {
        TBSYS_LOG(ERROR, "invalid argument m is %p", m);
      }
      else if (NULL == m->input)
      {
        TBSYS_LOG(ERROR, "invalide argument m->input is %p", m->input);
      }
      else
      {
        if ((len = static_cast<int32_t>(m->input->last - m->input->pos)) >= OB_MYSQL_PACKET_HEADER_SIZE)
        {
          //1. decode length from net buffer
          //2. decode seq from net buffer
          ObMySQLUtil::get_uint3(m->input->pos, pkt_len);
          ObMySQLUtil::get_uint1(m->input->pos, pkt_seq);

          //message has enough buffer
          if (pkt_len <= m->input->last - m->input->pos)
          {
            ObMySQLUtil::get_uint1(m->input->pos, pkt_type);
            //利用message带的pool进行应用层内存的分配
            buffer = reinterpret_cast<char*>(onev_pool_alloc(m->pool,
                                                              static_cast<uint32_t>(sizeof(ObMySQLCommandPacket) + pkt_len)));
            if (NULL == buffer)
            {
              TBSYS_LOG(ERROR, "alloc packet buffer(length=%lu) from m->pool failed", sizeof(ObMySQLCommandPacket) + pkt_len);
            }
            else
            {
              TBSYS_LOG(DEBUG, "alloc packet buffer length = %lu", sizeof(ObMySQLCommandPacket) + pkt_len);
              packet = new(buffer)ObMySQLCommandPacket();
              packet->set_header(pkt_len, pkt_seq);
              packet->set_type(pkt_type);
              packet->set_receive_ts(tbsys::CTimeUtil::getTime());
              memcpy(buffer + sizeof(ObMySQLCommandPacket), m->input->pos, pkt_len - 1);
              packet->get_command().assign(buffer + sizeof(ObMySQLCommandPacket), pkt_len - 1);
              TBSYS_LOG(DEBUG, "decode comand packet command is \"%.*s\"", packet->get_command().length(),
                        packet->get_command().ptr());
              if (PACKET_RECORDER_FLAG)
              {
                // record the packet to FIFO stream if required
                ObMySQLServer* server = reinterpret_cast<ObMySQLServer*>(m->c->handler->user_data);
                ObMySQLCommandPacketRecord record;
                record.socket_fd_ = m->c->fd;
                record.cseq_ = m->c->seq;
                record.addr_ = m->c->addr;
                record.pkt_length_ = pkt_len;
                record.pkt_seq_ = pkt_seq;
                record.cmd_type_ = pkt_type;
                struct iovec buffers[2];
                buffers[0].iov_base = &record;
                buffers[0].iov_len = sizeof(record);
                buffers[1].iov_base = m->input->pos;
                buffers[1].iov_len = pkt_len - 1;
                int err = OB_SUCCESS;
                if (OB_SUCCESS != (err = server->get_packet_recorder().push(buffers, 2)))
                {
                  TBSYS_LOG(WARN, "failed to record MySQL packet, err=%d", err);
                }
              }
              m->input->pos += pkt_len - 1;
            }
          }
          else
          {
            m->next_read_len = static_cast<int>(pkt_len - (m->input->last - m->input->pos));
            TBSYS_LOG(DEBUG, "not enough data in message, packet length = %u, data in message is %ld",
                      pkt_len, m->input->last - m->input->pos);
            m->input->pos -= OB_MYSQL_PACKET_HEADER_SIZE;
          }
        }
      }
      return packet;
    }

    int ObMySQLCallback::process(onev_request_e* r)
    {
      int ret = ONEV_OK;

      if (NULL == r)
      {
        TBSYS_LOG(ERROR, "request is NULL, r= %p", r);
        ret = ONEV_BREAK;
      }
      else if (NULL == r->ipacket)
      {
        TBSYS_LOG(ERROR, "reqeust is NULL, r->ipacket is %p", r->ipacket);
        ret = ONEV_BREAK;
      }
      else if (ONEV_AGAIN == r->retcode)  //wakeup request thread called when send result set sync
      {
        //ONEV_AGAIN说明后续服务器端还有包需要发给客户端
        if (NULL != r->client_wait)
        {
          if (r->ms->c->conn_has_error == 1)
          {
            r->client_wait->status = ONEV_CONN_CLOSE;
          }
          onev_client_wait_wakeup_request(r);
          ret = ONEV_AGAIN;
        }
      }
      else
      {
        ObMySQLServer* server = reinterpret_cast<ObMySQLServer*>(r->ms->c->handler->user_data);
        ObMySQLCommandPacket* packet = reinterpret_cast<ObMySQLCommandPacket*>(r->ipacket);
        TBSYS_LOG(DEBUG, "handle packet command=%.*s", packet->get_command().length(), packet->get_command().ptr());
        packet->set_request(r);
        onev_pool_set_lock(r->ms->pool);
        if (packet->get_command_length() + static_cast<int32_t>(sizeof(ObMySQLCommandPacket)) > common::OB_MAX_THREAD_BUFFER_SIZE)
        {
          //由于post_packet在工作线程中也会用到，这里只能先加引用计数
          r->ms->c->pool->ref++;
          onev_atomic_inc(&r->ms->pool->ref);
          TBSYS_LOG(ERROR, "packet is too large, greater than %d, size of request packet=%d, ret=%d",
                  common::OB_MAX_THREAD_BUFFER_SIZE,
                  packet->get_command_length() + static_cast<int32_t>(sizeof(ObMySQLCommandPacket)),
                  OB_SIZE_OVERFLOW);
          uint8_t number = packet->get_packet_header().seq_;
          number++;
          ObMySQLErrorPacket epacket;
          ObString err_msg = ObString::make_string("packet is too large");
          if (OB_SUCCESS != (ret = epacket.set_oberrcode(OB_SIZE_OVERFLOW)))
          {
            TBSYS_LOG(WARN, "set err code to error packet failed, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = epacket.set_message(err_msg)))
          {
            TBSYS_LOG(WARN, "set error message to error packet failed, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = server->post_packet(r, &epacket, number)))
          {
            TBSYS_LOG(ERROR, "failed to send error packet to mysql client(%s)", get_peer_ip(r));
          }
          else
          {
            ret = ONEV_AGAIN;
            TBSYS_LOG(DEBUG, "send error packet to mysql client(%s) succ", get_peer_ip(r));
          }
        }
        else
        {
          //just push packet into queue
          ret = server->handle_packet(packet);
          if (OB_SUCCESS != ret)
          {
            //请求处理结束，返回ONEV_OK
            ret = ONEV_OK;
            TBSYS_LOG(WARN, "can not push packet(src is %s, ptype is %u) to packet queue",
                      inet_ntoa_r(r->ms->c->addr), packet->get_type());
          }
          else
          {
            r->ms->c->pool->ref++;
            onev_atomic_inc(&r->ms->pool->ref);
            ret = ONEV_AGAIN;
          }
        }
      }
      return ret;
    }

    int ObMySQLCallback::on_connect(onev_connection_e* c)
    {
      int ret = ONEV_OK;
      if (NULL == c || NULL == c->handler)
      {
        TBSYS_LOG(WARN, "invalid argument c or c->handler is NULL");
        ret = ONEV_ERROR;
      }
      else
      {
        ObMySQLServer* server = reinterpret_cast<ObMySQLServer*>(c->handler->user_data);
        ret = server->login_handler(c);
        if (OB_SUCCESS != ret)
        {
          ret = ONEV_ERROR;
          TBSYS_LOG(WARN, "login faild");
        }
        else
        {
          ret = ONEV_OK;
        }
      }
      return ret;
    }

    int ObMySQLCallback::on_disconnect(onev_connection_e* c)
    {
      int ret = ONEV_OK;
      if (NULL == c || NULL == c->handler)
      {
        TBSYS_LOG(WARN, "invalid argument c or c->handler is NULL");
        ret = ONEV_ERROR;
      }
      else
      {
        if (c->reconn_fail > 10)
        {
          c->auto_reconn = 0;
        }
        ObMySQLServer* server = reinterpret_cast<ObMySQLServer*>(c->handler->user_data);
        ret = server->get_session_mgr()->erase(c->seq);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "submit async delete request, session key is %d ret is %d", c->seq, ret);
          //generate a async request to delete session later
          ret = server->submit_session_delete_task(c->seq);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "submit session delete task failed session key is %d", c->seq);
            ret = ONEV_ERROR;
          }
          else
          {
            ret = ONEV_OK;
          }
        }
        else
        {
          OB_STAT_INC(OBMYSQL, SUCC_LOGOUT_COUNT);
          TBSYS_LOG(INFO, "client disconnect, session key is %d, fd is %d", c->seq, c->fd);
        }
        if (PACKET_RECORDER_FLAG)
        {
          ObMySQLCommandPacketRecord record;
          record.socket_fd_ = c->fd;
          record.cseq_ = c->seq;
          record.addr_ = c->addr;
          record.pkt_seq_ = 0;
          record.pkt_length_ = 0;
          record.cmd_type_ = COM_END;
          record.obmysql_type_ = OBMYSQL_LOGOUT;
          struct iovec buffers;
          buffers.iov_base = &record;
          buffers.iov_len = sizeof(record);
          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = server->get_packet_recorder().push(&buffers, 1)))
          {
            TBSYS_LOG(WARN, "failed to record MySQL packet, err=%d", err);
          }
        }

      }
      return ret;
    }

    uint64_t ObMySQLCallback::get_packet_id(onev_connection_e* c, void* packet)
    {
      UNUSED(c);
      UNUSED(packet);
      return 0;
    }

    int ObMySQLCallback::clean_up(onev_request_e *r, void *apacket)
    {
      int ret = ONEV_OK;
      UNUSED(apacket);
      //ObMySQLCommandPacket *packet = reinterpret_cast<ObMySQLCommandPacket*>(r->ipacket);
      //TBSYS_LOG(INFO, "c= %p r=%p m=%p sql=%.*s", r->ms->c , r, r->ms, packet->get_command().length(), packet->get_command().ptr());
      if (NULL != r && NULL != r->client_wait)
      {
        r->client_wait->status = ONEV_CONN_CLOSE;
        onev_client_wait_wakeup(r->client_wait);
      }
      return ret;
    }
  }
}
