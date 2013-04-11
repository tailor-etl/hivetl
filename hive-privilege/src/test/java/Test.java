

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.ql.parse.defined.LoadPrivelegeFile;

public class Test {
	
	public static final AtomicInteger at=new AtomicInteger();

	/** 
	 * @Title: main 
	 * @Description: TODO(������һ�仰�����������������) 
	 * @param @param args    �趨�ļ� 
	 * @return void    �������� 
	 * @throws 
	 */

	public static void main(String[] args) {
		LoadPrivelegeFile.getDatabasetablemaps();
		List<Runnable> list=new ArrayList<Runnable>();
		CountDownLatch thread = new CountDownLatch(101);
	    for(int i=0;i<50;i++){
	    	list.add(new ThreadTest(thread));
	    }
	    list.add(new ThreadTest1(thread));
	    for(int i=0;i<50;i++){
	    	list.add(new ThreadTest(thread));
	    }
	    
        System.out.println(list.size());
	    for(Runnable r:list)
	    Executors.newFixedThreadPool(20).submit(r);
	    		try {
					thread.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	}
	
	static class ThreadTest implements Runnable{

		public CountDownLatch lat=null;
		
		public ThreadTest(CountDownLatch lat){
			this.lat=lat;
		}
		@Override
		public void run() {
			//System.out.println(System.currentTimeMillis()+"thread");
			try{
				HashSet<String> ss=(HashSet<String>) LoadPrivelegeFile.getDatabasetablemaps().get("default");
				int k=at.getAndIncrement();
				if(ss==null){
					System.out.println("ddd"+k);
				}else{
				System.out.println("oo"+System.currentTimeMillis()+ss.toString()+k);
				}
			}catch (Throwable e) {
				System.out.println(e.getMessage());
			}
			finally{
				lat.countDown();
			}
		}
		
	}
	
	static class ThreadTest1 implements Runnable{
		public CountDownLatch lat=null;
		@Override
		public void run() {
			//System.out.println(LoadPrivelegeFile.updateMaps()+"--"+at.getAndIncrement());
			lat.countDown();
		}
		public ThreadTest1(CountDownLatch lat){
			this.lat=lat;
		}
		
	}

}
