package org.pingcap.ticdc.cdc.value;

public class TicdcEventColumn {
    private int t;// type
    private boolean h; // Where Handle	Bool	表示该列是否可以作为 Where 筛选条件，当该列在表内具有唯一性时，Where Handle 为 true
    private String name; // column name
    private Object v; // v

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getT() {
        return t;
    }

    public void setT(int t) {
        this.t = t;
    }

    public boolean isH() {
        return h;
    }

    public void setH(boolean h) {
        this.h = h;
    }

    public Object getV() {
        return v;
    }

    public void setV(Object v) {
        this.v = v;
    }
}
