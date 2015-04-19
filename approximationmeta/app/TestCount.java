package approximationsearch.app;

public class TestCount {
	public static void main(String args[]) {
		Count r0 = new Count("Thread-0", 0);
		r0.start();
		
		Count r1 = new Count("Thread-1", 1);
		r1.start();
		
		Count r2 = new Count("Thread-2", 2);
		r2.start();		
	}
}
