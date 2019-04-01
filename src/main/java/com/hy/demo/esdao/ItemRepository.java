package com.hy.demo.esdao;

import com.hy.demo.pojo.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

/**
 * ElasticsearchCrudRepository 常用的CRUD
 * ElasticsearchRepository 可以使用匹配查找
 *
 */

public interface ItemRepository extends ElasticsearchRepository<Item,Long> {

    /**
     * 根据价格区间查询
     * @param price1
     * @param price2
     * @return
     */
    List<Item> findByPriceBetween(Long price1,Long price2);
}
