import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {
    private Text itemid;
    private DoubleWritable amount;

    public OrderBean(){
    }

    public OrderBean(Text id, DoubleWritable amount){
        this.set(id, amount);
    }

    public void set(Text id, DoubleWritable amount){
        this.itemid = id;
        this.amount = amount;
    }

    public Text getItemid(){
        return itemid;
    }

    public void setItemid(Text itemid){
        this.itemid = itemid;
    }

    public DoubleWritable getAmount(){
        return amount;
    }

    public void setAmount(DoubleWritable amount){
        this.amount = amount;
    }

    public void readFields(DataInput in) throws IOException{
        this.itemid = new Text(in.readUTF());
        this.amount = new DoubleWritable(in.readDouble());
    }

    public void write(DataOutput out) throws IOException{
        out.writeUTF(itemid.toString());
        out.writeDouble(amount.get());
    }

    public int compareTo(OrderBean o){
        int ret = this.itemid.compareTo(o.getItemid());
        if(ret == 0){
            ret = -this.amount.compareTo(o.getAmount());
        }
        return ret;
    }

    @Override
    public String toString(){
        return itemid.toString() + "\t" + amount.get();
    }
}