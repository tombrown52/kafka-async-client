package kafka.async.futures;

import java.util.concurrent.Future;

public interface SelectableFuture<T> extends Future<T> {

	void addSelector(FutureSelector<T> selector);
	void removeSelector(FutureSelector<T> selector);
	
}
