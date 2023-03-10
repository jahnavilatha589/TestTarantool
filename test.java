import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.time.ZonedDateTime;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.connection.TarantoolConnectionSelectionStrategyType;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.space.options.InsertOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyInsertOptions;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.protocol.Packable;


class test {
	static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = null;
	static {
		List<String> ipPorts = getipPorts();
		List<TarantoolServerAddress> ipPortsAddress = new ArrayList<>();
		for (int i = 0; i < ipPorts.size(); i++) {
			ipPortsAddress.add(new TarantoolServerAddress(ipPorts.get(i)));
		}
		client = TarantoolClientFactory.createClient()
				.withAddresses(ipPortsAddress)
				.withConnectTimeout(1000 * 15)
				.withReadTimeout(1000 * 15)
				.withRequestTimeout(1000 * 30)
				.withConnectionSelectionStrategy(TarantoolConnectionSelectionStrategyType.PARALLEL_ROUND_ROBIN)
				.withConnections(2)
				.build();
	}

	 static List<String> getipPorts(){
	   List<String> routerList = new ArrayList<>();
            // added 6 routers to routerList
 	   return routerList;
	 }

 	static void insertIntoTT(List<CompletableFuture<List<?>>> resultList,int i) throws Exception {
                JSONArray data = new JSONArray();
                data.put(i);
                data.put("jahnavi  " + i);
                data.put("ABCD");
                data.put("value");
		CompletableFuture<List<?>> result = client.call("nativePutData", data.toString());
		resultList.add(result);
	}

   public static void main(String args[]) throws Exception {
      int start = 0;
      int end = 5000000;
      int countDiffer = 500;
      long totalTimeTaken = 0;
      while(start < end) {
         long startTime  = System.currentTimeMillis(); 
         List<CompletableFuture<List<?>>> resultList = new ArrayList<>();   
         for(int i = start; i < start + countDiffer; i++) {
            insertIntoTT(resultList, i); 
         }
         for(int i = 0; i < resultList.size(); i++) {
            List<?> res = resultList.get(i).get();                      
         }

         start = start + countDiffer;
         totalTimeTaken = totalTimeTaken + System.currentTimeMillis() - startTime;
         if(start % 10000 == 0) {
            System.out.println("totalTimeTaken " + totalTimeTaken);
         }
      } 
   }
}

