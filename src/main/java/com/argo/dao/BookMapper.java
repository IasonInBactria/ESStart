package com.argo.dao;

import com.argo.domain.Book;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * @author YangY
 */
@Mapper
public interface BookMapper extends BaseMapper<Book> {
}
