import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StringAndInt implements Comparable<StringAndInt>, Writable {

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

	@Override
	public void readFields(DataInput arg0) throws IOException {
		tag=arg0.readUTF();
		nbOccurence=arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(tag);
		arg0.writeInt(nbOccurence);		
	}

}
