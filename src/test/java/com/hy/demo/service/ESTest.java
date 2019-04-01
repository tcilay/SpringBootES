package com.hy.demo.service;

import com.alibaba.fastjson.JSONObject;
import com.hy.demo.esdao.ItemRepository;
import com.hy.demo.pojo.Item;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ESTest {

    @Autowired
    private ElasticsearchTemplate template;
    @Autowired
    private ItemRepository itemRepo;

    @Test
    @Ignore
    public void testCreateIndex(){
        boolean success = template.createIndex(Item.class);
        if(success) System.out.println("创建成功");
        else System.out.println("创建失败");
    }

    @Test
    @Ignore
    public void testRemoveIndex(){
        boolean success = template.deleteIndex(Item.class);
        if(success){
            System.out.println("删除成功");
        }else{
            System.out.println("删除成功");
        }
    }

    /**
     * 修改和新增是同一个接口，区分的依据就是id
     */
    @Test
    public void testAdd(){
        Item item = new Item();
        item.setId(1L);
        item.setTitle("华为畅享9plus");
        item.setCategory("手机");
        item.setBrand("华为");
        item.setPrice(129900L);
        item.setImage("http://image.baidu.com/11111.jpg");
        itemRepo.save(item);
    }

    @Test
    public void addList(){
        List<Item> list = new ArrayList<Item>();
//        Item item = new Item();
//        item.setId(2L);
//        item.setTitle("小米note8");
//        item.setCategory("手机");
//        item.setBrand("小米");
//        item.setPrice(119900L);
//        item.setImage("http://image.baidu.com/22222.jpg");
//        list.add(item);
//        list.add(new Item(3L,"红米巴拉巴拉9","手机","小米",99900L,"http://image.baidu.com/22222.jpg"));
        list.add(new Item(4L, "小米手机7", "手机", "小米", 329900L, "http://image.baidu.com/44444.jpg"));
        list.add(new Item(5L, "坚果手机R1", "手机", "锤子", 369900L, "http://image.baidu.com/555555.jpg"));
        list.add(new Item(6L, "华为META10", "手机", "华为", 449900L, "http://image.baidu.com/66666.jpg"));
        list.add(new Item(7L, "小米Mix2S", "手机", "小米", 429900L, "http://image.baidu.com/77777.jpg"));
        list.add(new Item(8L, "荣耀V10", "手机", "华为", 279900L, "http://image.baidu.com/888888.jpg"));
        itemRepo.saveAll(list);
    }

    @Test
    public void find(){
        Iterable<Item> items = itemRepo.findAll(Sort.by("price").ascending());
        for(Item item : items){
            System.out.println(item);
        }
    }

    @Test
    public void findByPriceBetween(){
        List<Item> items = itemRepo.findByPriceBetween(200000L, 400000L);
        for(Item item : items){
            System.out.println(item);
        }
    }

    @Test
    public void testMatchQuery(){
        //构建查询条件
        NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder();
        //添加基本分词查询
        builder.withQuery(QueryBuilders.matchQuery("title","小米"));
        //搜索结果
        Page<Item> items = this.itemRepo.search(builder.build());
        //总条数
        long totalElements = items.getTotalElements();
        System.out.println("总条数："+totalElements);
        for (Item item : items){
            System.out.println(item);
        }


    }


    /**
     * termQuery 功能更强大，除了可以匹配字符串还可以匹配 int/long/double/float ....
     */
    @Test
    public void testTermQuery(){
        NativeSearchQueryBuilder query = new NativeSearchQueryBuilder();
        query.withQuery(QueryBuilders.termQuery("price",449900L));
        Page<Item> items = itemRepo.search(query.build());
        System.out.println(items.getTotalElements());
        for(Item item : items){
            System.out.println(item);
        }
    }

    /**
     * 布尔查询
     */
    @Test
    public void testBoolQuery(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("title","小米"))
                .must(QueryBuilders.matchQuery("brand","小米")));
        Page<Item> items = itemRepo.search(queryBuilder.build());
        for (Item item : items) System.out.println(item);
    }

    /**
     * 模糊查询
     */
    @Test
    public void testFuzzQuery(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.fuzzyQuery("title","小米米"));
        Page<Item> items = itemRepo.search(queryBuilder.build());
        for (Item item : items) System.out.println(item);
    }

    /**
     * 分页查询
     */
    @Test
    public void testPageQuery(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.termQuery("category","手机"));
        //分页
        int page = 1 ;
        int size = 2 ;

        queryBuilder.withPageable(PageRequest.of(page,size));
        Page<Item> items = this.itemRepo.search(queryBuilder.build());
        long total = items.getTotalElements();
        System.out.println("总条数："+total);
        System.out.println("总页数："+items.getTotalPages());
        System.out.println("当前页："+ items.getNumber());
        System.out.println("每页大小："+items.getSize());
        for (Item item : items) System.out.println(item);

    }


    /**
     * 查询排序
     */
    @Test
    public void testQueryAndSort(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.termQuery("category","手机"));
        //排序
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.ASC));

        Page<Item> items = this.itemRepo.search(queryBuilder.build());
        System.out.println("总条数："+items.getTotalElements());
        for (Item item : items) System.out.println(item);
    }

    /**
     * 按照品牌brand进行分组
     *
     * （1）统计某个字段的数量
     *   ValueCountBuilder vcb=  AggregationBuilders.count("count_uid").field("uid");
     * （2）去重统计某个字段的数量（有少量误差）
     *  CardinalityBuilder cb= AggregationBuilders.cardinality("distinct_count_uid").field("uid");
     * （3）聚合过滤
     * FilterAggregationBuilder fab= AggregationBuilders.filter("uid_filter").filter(QueryBuilders.queryStringQuery("uid:001"));
     * （4）按某个字段分组
     * TermsBuilder tb=  AggregationBuilders.terms("group_name").field("name");
     * （5）求和
     * SumBuilder  sumBuilder=	AggregationBuilders.sum("sum_price").field("price");
     * （6）求平均
     * AvgBuilder ab= AggregationBuilders.avg("avg_price").field("price");
     * （7）求最大值
     * MaxBuilder mb= AggregationBuilders.max("max_price").field("price");
     * （8）求最小值
     * MinBuilder min=	AggregationBuilders.min("min_price").field("price");
     * （9）按日期间隔分组
     * DateHistogramBuilder dhb= AggregationBuilders.dateHistogram("dh").field("date");
     * （10）获取聚合里面的结果
     * TopHitsBuilder thb=  AggregationBuilders.topHits("top_result");
     * （11）嵌套的聚合
     * NestedBuilder nb= AggregationBuilders.nested("negsted_path").path("quests");
     * （12）反转嵌套
     * AggregationBuilders.reverseNested("res_negsted").path("kps ");
     */
    @Test
    public void testQueryAgg(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 不查询任何结果
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""},null));
        // 1、添加一个新的聚合，聚合类型为terms，聚合名称为brands，聚合字段为brand
        queryBuilder.addAggregation(AggregationBuilders.terms("brands").field("brand"));
        // 2、查询,需要把结果强转为AggregatedPage类型
        AggregatedPage<Item> items = (AggregatedPage<Item>) this.itemRepo.search(queryBuilder.build());
        // 3、解析
        // 3.1、从结果中取出名为brands的那个聚合，
        // 因为是利用String类型字段来进行的term聚合，所以结果要强转为StringTerm类型
        StringTerms aggs = (StringTerms)items.getAggregation("brands");
        // 3.2、获取桶
        List<StringTerms.Bucket> buckets = aggs.getBuckets();
        // 3.3、遍历
        for (StringTerms.Bucket bucket : buckets){
            // 3.4、获取桶中的key，即品牌名称
            System.out.println(bucket.getKeyAsString());
            // 3.5、获取桶中的文档数量
            System.out.println(bucket.getDocCount());
        }


    }


    /**
     * 嵌套聚合，求平均值
     */
    @Test
    public void testQuerySubAgg(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //不查询任何结果
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{},null));
        //1、添加一个新的聚合，聚合类型为terms，聚合名称为brands，聚合字段为brand
        queryBuilder.addAggregation(AggregationBuilders
                .terms("brands").field("brand")
                // 在品牌聚合桶内进行嵌套聚合，求平均值
                .subAggregation(AggregationBuilders.avg("priceAvg").field("price"))
        );
        // 2、查询,需要把结果强转为AggregatedPage类型
        AggregatedPage<Item> aggPage = (AggregatedPage<Item>) this.itemRepo.search(queryBuilder.build());
        // 3、解析
        // 3.1、从结果中取出名为brands的那个聚合，
        // 因为是利用String类型字段来进行的term聚合，所以结果要强转为StringTerm类型
        StringTerms agg = (StringTerms) aggPage.getAggregation("brands");
        // 3.2、获取桶
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        // 3.3、遍历
        for (StringTerms.Bucket bucket : buckets) {
            // 3.4、获取桶中的key，即品牌名称  3.5、获取桶中的文档数量
            System.out.println(bucket.getKeyAsString() + "，共" + bucket.getDocCount() + "台");

            // 3.6.获取子聚合结果：
            InternalAvg avg = (InternalAvg) bucket.getAggregations().asMap().get("priceAvg");
            System.out.println("平均售价：" + avg.getValue());
        }
    }




}
