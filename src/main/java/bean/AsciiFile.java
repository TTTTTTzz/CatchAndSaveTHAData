package bean;

import java.sql.Timestamp;

public class AsciiFile {
    private int id;
    private String name;
    private long size;
    private Timestamp downloadTime;
    private Timestamp dataTime;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public Timestamp getDataTime() {
        return dataTime;
    }

    public void setDataTime(Timestamp dataTime) {
        this.dataTime = dataTime;
    }

    public Timestamp getDownloadTime() {
        return downloadTime;
    }

    public void setDownloadTime(Timestamp downloadTime) {
        this.downloadTime = downloadTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    @Override
    public String toString(){
        return "[Id:"+this.id+
                ",File Name:"+this.name+
                ",File Size:"+this.size+
                "kb,Download Time:"+this.downloadTime+
                ",Data Time:"+this.dataTime+"]\n";
    }


}
