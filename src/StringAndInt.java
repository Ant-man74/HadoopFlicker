public class StringAndInt implements Comparable<StringAndInt> {

	String tag ;
	int nbOccurence;
	
	public StringAndInt(String tag, int nbOccurence) {
		super();
		this.tag = tag;
		this.nbOccurence = nbOccurence;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public int getNbrOccurance() {
		return nbOccurence;
	}

	public void setNbrOccurance(int nbrOccurance) {
		this.nbOccurence = nbrOccurance;
	}

	@Override
	public int compareTo(StringAndInt toCompare) {		
		return  toCompare.nbOccurence - this.nbOccurence;
	}

}
