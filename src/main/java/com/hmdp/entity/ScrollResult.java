package com.hmdp.entity;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult {
    private List<?> list;
    private long time;
    private Integer offset;
}
