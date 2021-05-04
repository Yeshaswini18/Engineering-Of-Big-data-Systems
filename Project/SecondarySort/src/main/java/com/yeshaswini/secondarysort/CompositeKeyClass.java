package com.yeshaswini.secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyClass implements WritableComparable<CompositeKeyClass> {
    private String productionCompany;
    private String year;

    public CompositeKeyClass() {
    }

    public CompositeKeyClass(String productionCompany, String year) {
        this.productionCompany = productionCompany;
        this.year = year;
    }

    public String getProductionCompany() {
        return productionCompany;
    }

    public void setProductionCompany(String productionCompany) {
        this.productionCompany = productionCompany;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public int compareTo(CompositeKeyClass o) {
        int res = this.year.compareTo(o.getYear());
        return (res < 0) ? -1 : (res == 0) ? 0 : 1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(productionCompany);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.productionCompany = dataInput.readUTF();
        this.year = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return productionCompany +  " | " + year ;
    }
}
