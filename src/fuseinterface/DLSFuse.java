package fuseinterface;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;


import fuseCache.MemoryCache;

import stork.ad.Ad;
import stork.dls.config.DLSConfig;
import stork.dls.io.local.DBCache;
import stork.dls.service.prefetch.DLSProxyInfo;
import stork.dls.stream.DLSListingTask;
import stork.dls.util.DLSLog;
import stork.dls.util.DLSLogTime;


import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;
import net.fusejna.util.FuseFilesystemAdapterFull;

public class DLSFuse extends FuseFilesystemAdapterFull{
    public static boolean DEBUG_PRINT;
    private static DLSLog logger = DLSLog.getLog(DLSFuse.class.getName());
	private static MemoryCache concurrentCache;
	final static int MAXIMUM_TICKET = 2000;
	
	//private final static String DLSHOST = "http://didclab-ws2.cse.buffalo.edu:8080/DirectoryListingService/rest/dls/list";
	public final static String DLSHOST = "http://192.168.1.106:8080/DirectoryListingService/rest/dls/list";
	public static final String USER_AGENT = "Mozilla/5.0";
	//private final static String ROOT = "ftp://ftp.mozilla.org";
	//private final static String ROOT = "ftp://ftp.cse.buffalo.edu";
	//private final static String ROOT = "ftp://128.205.39.42";
	private static String ROOT = "";
	private static String mountpath = "";
	public static String proxy = null;
	public static String remotehost = "";
	public static String protocol = "";
	/**
	 * init/start the singleton in-Memory cache.
	 */
	public static void INIT() throws Exception{
		concurrentCache =  new MemoryCache();
	}
	
	
	public void FuseClientInit() throws Exception{
    	String name = "DLS.conf";
    	String ip = "";
    	String hostname = "";
    	try{
	    	ClassLoader cl = this.getClass().getClassLoader();
			URL url = cl.getResource(name);
			Ad ad = Ad.parse(url.openStream());
			concurrentCache =  new MemoryCache();
			DBCache.BDB_PATH = ad.get("conf.bdb_path", DBCache.BDB_PATH);
			MemoryCache.inMemoryCache_capacity = ad.getInt("conf.inmemorycache_capacity", MemoryCache.inMemoryCache_capacity);
			DLSConfig.CC_LISTING = ad.getBoolean("conf.cc_listing", DLSConfig.CC_LISTING);
			DLSConfig.DLS_CONCURRENCY_STREAM = ad.getInt("conf.dls_concurrency_stream", DLSConfig.DLS_CONCURRENCY_STREAM);
			DLSConfig.DLS_PIPE_CAPACITY = ad.getInt("conf.dls_pipe_capacity", DLSConfig.DLS_PIPE_CAPACITY);
			ip = ad.get("ip", "0.0.0.0");
			hostname = ad.get("hostname", "didclab@Davis Hall");
    	}catch (Exception ex){
			ex.printStackTrace();
			logger.debug("fail to open DLS.conf ~!");
			System.exit(1);
		}
		System.out.println("{{ DLS on: " + ip + " " + hostname );
		System.out.println("system listing on control channel: " + DLSConfig.CC_LISTING);
		System.out.println("dls.conf.BDB_PATH: " + DBCache.BDB_PATH);
		System.out.println("MemoryCache.inMemoryCache_capacity: " + MemoryCache.inMemoryCache_capacity);
		System.out.println("DLSConfig.DLS_CONCURRENCY_STREAM: " + DLSConfig.DLS_CONCURRENCY_STREAM);
		System.out.println("DLSConfig.DLS_PIPE_CAPACITY: " + DLSConfig.DLS_PIPE_CAPACITY);
		System.out.println("init all the components }}");    	
		try{
			stork.dls.config.DebugConfig.DebugPrint();
			stork.dls.stream.DLSIOAdapter.INIT();
			stork.dls.service.prefetch.DLSThreadsManager.INIT();
		}catch (Exception e) {
			logger.debug("fail to init all the components~!");
			e.printStackTrace();
			System.exit(1);
		}
    }
    	
	HttpClient client = HttpClientBuilder.create().build();
	public Ad sendrecv(String path) throws Exception{
		URI uri = null;
		Ad adResult = null;
		synchronized(client){
			//HttpClient client = HttpClientBuilder.create().build();
			//uri = new URIBuilder(DLSHOST).addParameter("URI", path).build();
			uri = new URIBuilder(DLSHOST).addParameter("URI", path).addParameter("proxyCertificate", proxy).build();
			
			HttpGet request = new HttpGet(uri);
			request.addHeader("User-Agent", USER_AGENT);
			logger.debug("sending: "+path);
			HttpResponse response = client.execute(request);
			logger.debug("recving: "+path);
			int responseCode = response.getStatusLine().getStatusCode();
			if(200 == responseCode){
				adResult = Ad.parse(response.getEntity().getContent());
			}
			HttpEntity entity = response.getEntity();
			InputStream is = entity.getContent();
			is.close();			
			if(200 != responseCode){
				adResult = null;
			}
		}
		return adResult;	
	}

	void printAdMessage(Ad ad){
		final Attributes partenattr = ad.unmarshalAs(Attributes.class);
		for(Attributes sub: partenattr.files){
			System.out.print("name: " + sub.name + "; ");
			//System.out.print("host: " + sub.host + " ");
			
			System.out.print("mdtm: " + sub.mdtm + "; ");
			if(sub.owner.equals("?")) sub.owner = "ftp";
			System.out.print("owner: " + sub.owner + "; ");
			if(sub.group.equals("?")) sub.group = "ftp";
			System.out.print("group: " + sub.group + "; ");
			
			System.out.print("size: " + sub.size + "; ");
			System.out.println("perm: " + sub.perm + "; ");
		}
	}

	boolean isHidden(final String path){
		char[] cstr = path.toCharArray();
		final int len = cstr.length;
		for(int i = 0; i < len; i++){
			if('/' == cstr[i]) {
				continue;
			}else if(' ' == cstr[i]) {
				continue;
			}else {
				if('.' == cstr[i]){
					return true;
				}
				return false;
			}
		}
		return false;
	}
	
	String readwriteCache (String path) throws Exception{
		long threadID = Thread.currentThread().getId()%MAXIMUM_TICKET;
		URI uri = new URI(ROOT+path);
		DLSProxyInfo dlsproxy = null;
		final boolean forceRefresh = false;
		final boolean enablePrefetch = false;
		final String zone = null;
		final String resource = null;
		DLSListingTask fetchingtask = new DLSListingTask(threadID, uri, dlsproxy, forceRefresh, enablePrefetch, proxy, zone, resource);
		String adstr = null; 
		adstr = concurrentCache.read(threadID, fetchingtask, null);

		Ad rootAd = null;
		if(null == adstr){
			rootAd = sendrecv(ROOT+path);
			/*
			if(path.equals("/.Xauthority")){
				logger.debug("send "+ ROOT+path + " " + "recv" + rootAd.toJSON());
			}*/
			if(null != rootAd){
				Attributes attr = rootAd.unmarshalAs(Attributes.class);
				concurrentCache.write(threadID, fetchingtask, rootAd.toJSON(), null);
				if(attr.file){
					return rootAd.toJSON();		
				}
				for(Ad subAd : rootAd.getAds("files")){
				//for(Attributes sub: attr.files){
					String name = subAd.get("name");
					fetchingtask.setCachekey(path+name);
					adstr = subAd.toJSON();
					concurrentCache.write(threadID, fetchingtask, adstr, null);
				}
			}
			return rootAd.toJSON();
		}else{
			return adstr;
		}
	}
	
	String isFileType(final String path){
		File file = new File(path);
		String parent = file.getParent();
		if(null == parent || parent.equals("/")){
			parent = this.mountpath+"/";
		}else{
			parent = this.mountpath+parent+"/";
		}
		parent = parent.replaceAll("/+", "/");
		//logger.debug(" getattr 1" + " parent path: " + parent + " this:" +path);
		
		//check parent
		long threadID = Thread.currentThread().getId()%MAXIMUM_TICKET;
		URI uri;
		try {
			uri = new URI(ROOT+path);
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
			return null;
		}
		String adstr = null;
		String ABSPATH = mountpath+path+"/";
		ABSPATH = ABSPATH.replaceAll("/+", "/");
		/*
		try {
			uri = new URIBuilder(DLSHOST).addParameter("URI", remotehost+ABSPATH).build();
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
			return null;
		}*/
		uri = URI.create(remotehost+ABSPATH);
		DLSListingTask fetchingcurrent = new DLSListingTask(threadID, uri, null, false, true, proxy, null, null);
		//DLSListingTask fetchingcurrent = new DLSListingTask(uri.getHost(), ABSPATH);
		try {
			adstr = concurrentCache.read(threadID, fetchingcurrent, null);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		if(null != adstr){
			return adstr;
		}
		/*
		try {
			uri = new URIBuilder(DLSHOST).addParameter("URI", remotehost+parent).build();
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
			return null;
		}*/
		uri = URI.create(remotehost+parent);
		DLSListingTask fetchingparent = new DLSListingTask(threadID, uri, null, false, true, proxy, null, null);
		//DLSListingTask fetchingparent = new DLSListingTask(uri.getHost(), parent);
		adstr = null; 
		try {
			adstr = concurrentCache.read(threadID, fetchingparent, null);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
		if(null != adstr){
			Ad ad = Ad.parse(adstr);
			/*
			boolean found = false;
			try{			
				String nom = path+"//";
				nom = path.replaceAll("/+", "/");
				int len = nom.length();
				int begin = 0;
				int end = len;
				int i = len-1;
				if(nom.charAt(len-1) != '/'){
					end = len-1;
				}
				i = end-1;
				while(i >= begin){
					if(nom.charAt(i) == '/'){
						begin = i+1;
						break;
					}
					i--;
				}
				String val = nom.substring(begin, len);
				found = ad.has("name") && ad.get("name").equals(val);
				logger.debug("\t\t" + val + " of " + nom + " : " + found);
			}catch (Exception ex){
				ex.printStackTrace();
			}*/
			
			Attributes attr = ad.unmarshalAs(Attributes.class);
			for(Attributes subattr: attr.files){
				String abspath = parent+subattr.name+"/";
				abspath = abspath.replaceAll("/+", "/");
				if(subattr.file){
					if(abspath.equals(ABSPATH)){
						DLSListingTask task = new DLSListingTask(attr.host, abspath);
						Ad newad = new Ad();
						newad.put("host", subattr.host);
						newad.put("name", subattr.name);
						newad.put("mdtm", subattr.mdtm);
						newad.put("owner", subattr.owner);
						newad.put("group", subattr.group);
						newad.put("dir", subattr.dir);
						newad.put("file", subattr.file);
						newad.put("size", subattr.size);
						newad.put("perm", subattr.perm);
						adstr = newad.toJSON();
						try {
							concurrentCache.write(threadID, task, adstr, null);
						} catch (Exception e) {
							e.printStackTrace();
							return null;
						}
						return adstr;
					}
				}
			}
		}
		return null;
	}
	
	@Override
	public int getattr(final String path, final StatWrapper stat){
		logger.debug("[getattr]: path: " + path+ " arrives@ "+new DLSLogTime().timeStamp());
		/*
		if(isHidden(path)){
			logger.debug("skip: "+ path+ ", hidden files and directories");
			return 0;
		}*/
		String adstr = isFileType(path);
		if(null == adstr){
			try{
				 adstr = readwriteCache(path);
			}catch (Exception e){
				//e.printStackTrace();
				//logger.debug("getattr 2" + path + " " + " got exception~");
				return -ErrorCodes.ENOENT();
			}
		}
		Attributes attr = null;
		try{
			Ad ad = Ad.parse(adstr);
			attr = ad.unmarshalAs(Attributes.class);
			int perm = Integer.parseInt(Integer.toString(attr.perm), 8);
			logger.debug("path: " + path + ": perm: octal "+attr.perm + " decimal: " + perm);
			if(attr.dir){
				stat.setMode(NodeType.DIRECTORY,
				(perm & 0400) != 0, (perm & 0200) != 0, (perm & 0100) != 0,
				(perm & 0040) != 0, (perm & 0020) != 0, (perm & 0010) != 0,
				(perm & 0004) != 0, (perm & 0002) != 0, (perm & 0001) != 0
				);
			}else if(attr.file){
				stat.setMode(NodeType.FILE,
				(perm & 0400) != 0, (perm & 0200) != 0, (perm & 0100) != 0,
				(perm & 0040) != 0, (perm & 0020) != 0, (perm & 0010) != 0,
				(perm & 0004) != 0, (perm & 0002) != 0, (perm & 0001) != 0
				);				
			}
			stat.size(attr.size);
			logger.debug("[getattr]: path: " + path+ " leaves@ "+new DLSLogTime().timeStamp());
			return 0;
		}catch(Exception ex){
			logger.debug("[getattr]: path: " + path+ " leaves@ "+new DLSLogTime().timeStamp());
			logger.debug("getattr 3" + path + " " + " got exception~");
			return -ErrorCodes.ENOENT();
		}
	}
	
	@Override
	public int readdir(final String path, final DirectoryFiller filler){
		logger.debug("[readdir]: path: " + path+ " arrives@ "+new DLSLogTime().timeStamp());
		/*
		if(isHidden(path)){
			logger.debug("skip: "+ path+ ", hidden files and directories");
			return 0;
		}*/
		String adstr = null;
		try{
			 adstr = readwriteCache(path);
		}catch (Exception e){
			//e.printStackTrace();
			logger.debug("readdir 1" + path + " " + " got exception~");
			return -ErrorCodes.ENOENT();
		}
		Attributes attr = null;
		try{
			Ad ad = Ad.parse(adstr);
			attr = ad.unmarshalAs(Attributes.class);
			for(Attributes subattr: attr.files){
				//if(sub.file){
					filler.add(subattr.name);
				//}
			}
			logger.debug("[readdir]: path: " + path+ " leaves@ "+new DLSLogTime().timeStamp());
			return 0;
		}catch(Exception ex){
			logger.debug("[readdir]: path: " + path+ " leaves@ "+new DLSLogTime().timeStamp());
			logger.debug(path + "~");
			return -ErrorCodes.ENOENT();
		}
	}	
	
    private static String readEntireFile(String filename) throws IOException {
        FileReader in = new FileReader(filename);
        StringBuilder contents = new StringBuilder();
        char[] buffer = new char[4096];
        int read = 0;
        do {
            contents.append(buffer, 0, read);
            read = in.read(buffer);
        } while (read >= 0);
        return contents.toString();
    }	

	public static void main(String[] args) throws Exception{
		if(args.length < 2) {
			System.err.println("must take one parameter as mountpoint, 2nd is remote path");
			System.exit(1);
		}
			
		URI uri = URI.create(args[1]);
        protocol = uri.getScheme();
        remotehost = uri.getHost();
	    mountpath = uri.getPath();
		if(null != remotehost){
		    uri = new URI(protocol+ "://" + remotehost+mountpath);
		}else{
		    uri = new URI(protocol+ ":/" + mountpath);
		}
		remotehost = protocol+"://"+remotehost+"/";
	    ROOT = uri.toString();
		if(protocol.equalsIgnoreCase("gsiftp")){
			proxy = readEntireFile("/tmp/x509up_u1000");
		}		
		DLSFuse client = new DLSFuse();
		client.log(false);
		client.FuseClientInit();
/*
		long threadID = Thread.currentThread().getId()%MAXIMUM_TICKET;
		DLSProxyInfo dlsproxy = null;
		final boolean forceRefresh = false;
		final boolean enablePrefetch = false;
		final String zone = null;
		final String resource = null;
		DLSListingTask fetchingtask = new DLSListingTask(threadID, uri, dlsproxy, forceRefresh, enablePrefetch, proxy, zone, resource);
		String adstr = null; 
		adstr = concurrentCache.read(threadID, fetchingtask, null);
		if(null == adstr){
			concurrentCache.write(threadID, fetchingtask, "asasasa", null);
		}
		adstr = concurrentCache.read(threadID, fetchingtask, null);
*/
		try{
			client.mount(args[0]);
		}catch (Exception e){
			e.printStackTrace();
		}
		//DLSFuse.sendrecv("ftp://ftp.cse.buffalo.edu/");
		//DLSFuse.sendrecv("ftp://ftp.mozilla.org/");
	}
}