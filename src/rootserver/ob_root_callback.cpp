#include "ob_root_callback.h"
#include "tblog.h"
#include "rootserver/ob_root_worker.h"
<<<<<<< HEAD
#include "onev_struct.h"
=======
#include "easy_io_struct.h"
>>>>>>> refs/remotes/origin/master
#include "common/ob_packet.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace rootserver
  {
<<<<<<< HEAD
    int ObRootCallback::process(onev_request_e *r)
    {
      int ret = ONEV_OK;
      if (NULL == r || NULL == r->ipacket)
      {
        char buff[32];
        onev_addr_e addr = r->ms->c->addr;
=======
    int ObRootCallback::process(easy_request_t *r)
    {
      int ret = EASY_OK;
      if (NULL == r || NULL == r->ipacket)
      {
        char buff[32];
        easy_addr_t addr = r->ms->c->addr;
>>>>>>> refs/remotes/origin/master
        if (NULL == r)
        {
          TBSYS_LOG(ERROR, "request is empty, r = %p", r);
        }
        else
        {
          TBSYS_LOG(ERROR, "request is empty, r->ipacket = %p", r->ipacket);
        }
<<<<<<< HEAD
        TBSYS_LOG(ERROR, "receive packet from server:%s faild", onev_inet_addr_to_str(&addr, buff, 32));
        ret = ONEV_BREAK;
=======
        TBSYS_LOG(ERROR, "receive packet from server:%s faild", easy_inet_addr_to_str(&addr, buff, 32));
        ret = EASY_BREAK;
>>>>>>> refs/remotes/origin/master
      }
      else
      {
        ObRootWorker *worker = (ObRootWorker* )r->ms->c->handler->user_data;
        ObPacket* req = (ObPacket*) r->ipacket;
        TBSYS_LOG(DEBUG, "handle packet code is %d", req->get_packet_code());
        req->set_request(r);
        //handlePacket will handle those packet can not distributed to queue
        ret = worker->handlePacket(req);
        if (OB_SUCCESS == ret)
        {
          r->ms->c->pool->ref ++;
<<<<<<< HEAD
          onev_atomic_inc(&r->ms->pool->ref);
          onev_pool_set_lock(r->ms->pool);
          ret = ONEV_AGAIN;
=======
          easy_atomic_inc(&r->ms->pool->ref);
          easy_pool_set_lock(r->ms->pool);
          ret = EASY_AGAIN;
>>>>>>> refs/remotes/origin/master

        }
        else
        {
          TBSYS_LOG(WARN, "can not push packet(src is %s, pcode is %u) to packet queue", 
                    inet_ntoa_r(r->ms->c->addr), req->get_packet_code());
<<<<<<< HEAD
          ret = ONEV_OK;
=======
          ret = EASY_OK;
>>>>>>> refs/remotes/origin/master
        }
      }
      return ret;
    }
  }//namespace rootserver
}//namespace oceanbase

