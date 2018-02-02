/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_tbnet_callback.h is for what ...
 *
 * Version: ***: ob_tbnet_callback.h  Wed May 16 16:58:44 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */

#ifndef OB_TBNET_CALLBACK_H_
#define OB_TBNET_CALLBACK_H_

<<<<<<< HEAD
#include "onev_struct.h"
=======
#include "easy_io_struct.h"
>>>>>>> refs/remotes/origin/master

namespace oceanbase
{
  namespace common
  {
    class ObTbnetCallback
    {
      public:
<<<<<<< HEAD
        static void *  decode(onev_message_e *m);

        static int     encode(onev_request_e *r, void *packet);

        static int    batch_process(onev_message_e *m);

        static uint64_t get_packet_id(onev_connection_e *c, void *packet);

        static int shadow_process(onev_request_e*  r);

        static int on_disconnect(onev_connection_e* c);

        //session的回调函数，用于销毁session
        static int default_callback(onev_request_e* r);
=======
        static void *  decode(easy_message_t *m);

        static int     encode(easy_request_t *r, void *packet);

        static int    batch_process(easy_message_t *m);

        static uint64_t get_packet_id(easy_connection_t *c, void *packet);

        static int shadow_process(easy_request_t*  r);

        static int on_disconnect(easy_connection_t* c);

        //session的回调函数，用于销毁session
        static int default_callback(easy_request_t* r);
>>>>>>> refs/remotes/origin/master
    };
  }
}
#endif
