package stork.dls.stream.type;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Vector;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.globus.ftp.FileInfo;
import org.globus.ftp.exception.FTPReplyParseException;

import fuseinterface.DLSFuse;

import stork.ad.Ad;
import stork.dls.client.FuseClient;
import stork.dls.stream.DLSListingTask;
import stork.dls.stream.DLSStream;
import stork.dls.stream.DLSStreamPool;
import stork.dls.stream.DLSStream.CHANNEL_STATE;
import stork.dls.util.DLSLog;
/**
 * Wrapper of everything, like a tool class.<br>
 * Passive mode 
 * @author bing
 * @see DLSStream
 */
public class FuseStream extends DLSStream{
	public static boolean DEBUG_PRINT;
    private static DLSLog logger =
    		DLSLog.getLog(FuseStream.class.getName());	

	public static final int DEFAULT_FTP_PORT = 80;
	private HttpClient authclient = null;	
	
	public FuseStream(String streamkey) {
		super(streamkey);
	}public FuseStream(int id, String streamKey) {
	    super(id, streamKey);
    }
	
    final protected String DefaultPort(){
		port = DEFAULT_FTP_PORT;
		realprotocol = "httpclient";
		return realprotocol;
	}
   
	final protected synchronized void Authenticate(final DLSListingTask listingytask, 
	        String assignedThreadName, String path, String token) throws Exception {
		while(true){
		    if(spinlock.writeLock().tryLock()){
		    	try{
					authclient = HttpClientBuilder.create().build();
					logger.debug(authclient +" got created~");
			        this.isAvailable = true;
			        this.value = ONE_PIPE_CAPACITY;
		    	}finally{
		            spinlock.writeLock().unlock();
		        }
		        break;
		    }
		}
	}
	
	//final protected Vector<FileInfo> listCMD (String assignedThreadName, String root_path)throws Exception
	final protected Ad listCMD (DLSListingTask listingtask, String assignedThreadName, String listingFullPath)throws Exception{
		Ad adResult = null;
		FuseClient proxyclient = listingtask.getClient();
		synchronized(authclient){
			try{
				URI uri = new URIBuilder(DLSFuse.DLSHOST).addParameter("URI", DLSFuse.remotehost+listingFullPath).addParameter("proxyCertificate", DLSFuse.proxy).build();
				HttpGet request = new HttpGet(uri);
				request.addHeader("User-Agent", DLSFuse.USER_AGENT);
				logger.debug("listing on "+DLSFuse.remotehost+listingFullPath);
				logger.debug(authclient+"\t sending: "+listingFullPath+" request to DLS");
				HttpResponse response = authclient.execute(request);
				int responseCode = response.getStatusLine().getStatusCode();
				logger.debug(authclient+"\t recving: " +listingFullPath+ " request to DLS + code: " + responseCode);
				if(200 == responseCode){
					adResult = Ad.parse(response.getEntity().getContent());
				}
				HttpEntity entity = response.getEntity();
				InputStream is = entity.getContent();
				is.close();
				if(200 != responseCode){
					proxyclient.channelstate = CHANNEL_STATE.DC_IGNORE;
					throw new Exception();
				}
			}catch (Exception ex){
				//ex.printStackTrace();
				int existed = ex.toString().indexOf("500-500 Command failed");
				if(-1 != existed){
				    System.out.println(ex);
				    logger.debug("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + " cc stat: " + proxyclient.channelstate);
				}
				
			    if(proxyclient.channelstate == CHANNEL_STATE.CC_REVIVE){
			        /**
			         * set exception to all the threads which are still queuing.
			         * And let this thread which caused the CC_REVIVE to revive this cc.
			         */
			        logger.debug("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + " to do CC revive");
			        throw ex;
			    }else if(proxyclient.channelstate == CHANNEL_STATE.DC_RETRANSMIT){
			    	logger.debug("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + "TTL: " + listingtask.TTL +" DC migrate TTL");
			    	if(listingtask.TTL <= 0){
			    		proxyclient.channelstate = CHANNEL_STATE.DC_IGNORE;
			    		logger.debug("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + " DC migrate to DC ignore");
			    		//throw ex;
			    	}else{
				    	listingtask.TTL--;
				        logger.debug("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + " DC migrate");
				        throw ex;
			    	}
			    }else if(proxyclient.channelstate == CHANNEL_STATE.DC_IGNORE){
			    	adResult = null;
			        logger.debug("stream " + proxyclient.getStream().streamID + " " + assignedThreadName + listingFullPath + " DC ignore");
			    }
			}
		}
		return adResult;
	}
	@Override
	public void close(){
		try {
			finalize();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	/*
	@Override
	protected void finalize() throws Throwable {
	    authclient.close();
	    authclient = null;
		super.finalize();
	}*/

	@Override
	public void fillStreamPool(String streamkey) {
		int i = 0;
		for(; i < DLSStreamPool.NUM_CONCURRENCY_STREAM; i ++){
			DLSStream e = null;
			e = new FuseStream(i, streamkey);
			dls_StreamPool.streamList.add(e);
		}
	}

}
