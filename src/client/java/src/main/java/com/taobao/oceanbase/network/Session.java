<<<<<<< HEAD
package com.taobao.oceanbase.network;

import java.net.SocketAddress;

public interface Session {

	<T> T commit(byte[] message);

	SocketAddress getServer();

	boolean isConnected();

	void close();
=======
package com.taobao.oceanbase.network;

import java.net.SocketAddress;

public interface Session {

	<T> T commit(byte[] message);

	SocketAddress getServer();

	boolean isConnected();

	void close();
>>>>>>> refs/remotes/origin/master
}