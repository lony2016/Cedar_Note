#include "ob_update_callback.h"
#include "tblog.h"
#include "ob_update_server.h"
#include "common/ob_packet.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {     
<<<<<<< HEAD
    int ObUpdateCallback::process(onev_request_e* r)
    {
      int ret = ONEV_OK;
      if (NULL == r)
      {
        TBSYS_LOG(ERROR, "request is empty, r = %p", r);
        ret = ONEV_BREAK;
=======
    int ObUpdateCallback::process(easy_request_t* r)
    {
      int ret = EASY_OK;
      if (NULL == r)
      {
        TBSYS_LOG(ERROR, "request is empty, r = %p", r);
        ret = EASY_BREAK;
>>>>>>> refs/remotes/origin/master
      }
      else if (NULL == r->ipacket)
      {
        TBSYS_LOG(ERROR, "request is empty, r->ipacket = %p", r->ipacket);
<<<<<<< HEAD
        ret = ONEV_BREAK;
=======
        ret = EASY_BREAK;
>>>>>>> refs/remotes/origin/master
      }
      else
      {
        ObUpdateServer *server = reinterpret_cast<ObUpdateServer*>(r->ms->c->handler->user_data);
        ObPacket *req = reinterpret_cast<ObPacket*>(r->ipacket);
        req->set_request(r);
        r->ms->c->pool->ref ++;
<<<<<<< HEAD
        onev_atomic_inc(&r->ms->pool->ref);
        onev_pool_set_lock(r->ms->pool);
=======
        easy_atomic_inc(&r->ms->pool->ref);
        easy_pool_set_lock(r->ms->pool);
>>>>>>> refs/remotes/origin/master
        ret = server->handlePacket(req);
        if (OB_SUCCESS == ret)
        {
          // enqueue success
<<<<<<< HEAD
          ret = ONEV_AGAIN;
=======
          ret = EASY_AGAIN;
>>>>>>> refs/remotes/origin/master
        }
        else if (OB_ENQUEUE_FAILED == ret)
        {
          TBSYS_LOG(WARN, "can not push packet(src is %s, pcode is %u) to packet queue", 
                    inet_ntoa_r(r->ms->c->addr), req->get_packet_code());
          r->ms->c->pool->ref --;
<<<<<<< HEAD
          onev_atomic_dec(&r->ms->pool->ref);
          ret = ONEV_OK;
        }
        else /* OB_ERROR */
        {
          ret = ONEV_AGAIN;
=======
          easy_atomic_dec(&r->ms->pool->ref);
          ret = EASY_OK;
        }
        else /* OB_ERROR */
        {
          ret = EASY_AGAIN;
>>>>>>> refs/remotes/origin/master
        }
      }
      return ret;
    }
  }
}
