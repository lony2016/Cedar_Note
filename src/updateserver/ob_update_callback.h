/*
 *
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * ob_update_callback.h is for what ...
 *
 * Version: ***: ob_update_callback.h  Thu May 24 16:17:03 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want 
 *
 */
#ifndef OB_UPDATE_CALLBACK_H_
#define OB_UPDATE_CALLBACK_H_
<<<<<<< HEAD
#include "onev_struct.h"
=======
#include "easy_io_struct.h"
>>>>>>> refs/remotes/origin/master

namespace oceanbase
{
  namespace updateserver
  {
    class ObUpdateCallback
    {
    public:
<<<<<<< HEAD
      static int process(onev_request_e* r);
=======
      static int process(easy_request_t* r);
>>>>>>> refs/remotes/origin/master
    };
  }
}
#endif
