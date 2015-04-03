package approximationmeta.src;

public  class Accuracy implements Comparable<Accuracy>{
		public int total;
		public int distance;
		public Accuracy(){
			total =0;
			distance =0;
		}
		public int compareTo(Accuracy that){
			return that.total - this.total;
		} 
} 

