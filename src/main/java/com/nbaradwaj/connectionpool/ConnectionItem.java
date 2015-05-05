package com.nbaradwaj.connectionpool;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

public interface ConnectionItem {
	int STATE_NOT_IN_USE = 0;
	int STATE_IN_USE = 1;
	int STATE_EVICTED = 2;

	AtomicInteger state();
	
	Connection getConnection();
}
