package stork.dls.config;

import fuseinterface.DLSFuse;
import stork.dls.service.prefetch.DLSThreadsManager;
import stork.dls.service.prefetch.MyThreadPoolExecutor;
import stork.dls.service.prefetch.PrefetchingServices;
import stork.dls.service.prefetch.WorkerThread;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStreamManagement;
import stork.dls.stream.DLSStreamPool;
import stork.dls.stream.type.FuseStream;
/**
 * debug print Config
 * @author bing
 */
public class DebugConfig {
	/**
	 * for debug
	 */
	public static void DebugPrint(){
		PrefetchingServices.DEBUG_PRINT = true;
		DLSFuse.DEBUG_PRINT = true;
		FuseStream.DEBUG_PRINT = true;
		DLSStreamPool.DEBUG_PRINT = true;//false;true;
		PrefetchingServices.DEBUG_PRINT = false;//true;
		
		WorkerThread.DEBUG_PRINT = false;
		DLSThreadsManager.DEBUG_PRINT = false;	
		
		//DLS network protocol client: false
		
		//PipeStream.DEBUG_PRINT = false;
		
		MyThreadPoolExecutor.DEBUG_PRINT = false;//true;
		
		//all the protocols stream class
		DLSStream.DEBUG_PRINT = false;//true;
		
		DLSStreamManagement.DEBUG_PRINT = false;
	}
	/**
	 * time measurement config
	 */
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}