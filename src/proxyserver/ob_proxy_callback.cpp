#include "ob_proxy_callback.h"
#include "tblog.h"
#include "ob_proxy_server.h"
#include "common/ob_packet.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace proxyserver
  {
<<<<<<< HEAD
    int ObProxyCallback::process(onev_request_e *r)
    {
      int ret = ONEV_OK;
=======
    int ObProxyCallback::process(easy_request_t *r)
    {
      int ret = EASY_OK;
>>>>>>> refs/remotes/origin/master

      if (NULL == r)
      {
        TBSYS_LOG(WARN, "request is NULL, r = %p", r);
<<<<<<< HEAD
        ret = ONEV_BREAK;
=======
        ret = EASY_BREAK;
>>>>>>> refs/remotes/origin/master
      }
      else if (NULL == r->ipacket)
      {
        TBSYS_LOG(WARN, "request is NULL, r->ipacket = %p", r->ipacket);
<<<<<<< HEAD
        ret = ONEV_BREAK;
=======
        ret = EASY_BREAK;
>>>>>>> refs/remotes/origin/master
      }
      else
      {
        ObProxyServer* server = (ObProxyServer*)r->ms->c->handler->user_data;
        ObPacket* packet = (ObPacket*)r->ipacket;
        packet->set_request(r);
        //handle_request will send response
        if (OB_REQUIRE_HEARTBEAT == packet->get_packet_code())
        {
          server->handle_request(packet);
<<<<<<< HEAD
          ret = ONEV_OK;
=======
          ret = EASY_OK;
>>>>>>> refs/remotes/origin/master
        }
        else
        {
          ret = server->handlePacket(packet);
          if (OB_SUCCESS == ret)
          {
            r->ms->c->pool->ref++;
<<<<<<< HEAD
            onev_atomic_inc(&r->ms->pool->ref);
            onev_pool_set_lock(r->ms->pool);
            ret = ONEV_AGAIN;
          }
          else
          {
            ret = ONEV_OK;
=======
            easy_atomic_inc(&r->ms->pool->ref);
            easy_pool_set_lock(r->ms->pool);
            ret = EASY_AGAIN;
          }
          else
          {
            ret = EASY_OK;
>>>>>>> refs/remotes/origin/master
            TBSYS_LOG(WARN, "can not push packet(src is %s, pcode is %u) to packet queue",
                      inet_ntoa_r(r->ms->c->addr), packet->get_packet_code());

          }
        }
      }
      return ret;
    }
  }
}
