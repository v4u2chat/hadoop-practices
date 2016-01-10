package com.rajesh.hadoop.custom.datatype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Txn implements Writable
{

	private String product;
	private int amount;
	private String txnDate;

	public String getProduct()
	{
		return product;
	}

	public void setProduct(String product)
	{
		this.product = product;
	}

	public int getAmount()
	{
		return amount;
	}

	public void setAmount(int amount)
	{
		this.amount = amount;
	}

	public String getTxnDate()
	{
		return txnDate;
	}

	public void setTxnDate(String txnDate)
	{
		this.txnDate = txnDate;
	}

	public void readFields(DataInput in) throws IOException
	{
		this.amount = in.readInt();
		this.product = in.readUTF();
		this.txnDate = in.readUTF();
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeInt(amount);
		out.writeUTF(product);
		out.writeUTF(txnDate);
	}

}
