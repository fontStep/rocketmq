package com.pojo;

/**
 * @ClassName: OrderDemo
 * @Description: TOOD
 * @version: v1.0.0
 * @author: wjw
 * @date: 2018/7/21 21:08
 */
public class OrderDemo implements java.io.Serializable {

    private Long id;

    private String desc;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return "OrderDemo{" +
                "id=" + id +
                ", desc='" + desc + '\'' +
                '}';
    }
}
