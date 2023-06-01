package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_LIST_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        //1.从redis查询商铺类型缓存
        Set<String> shopSetJsonSet = stringRedisTemplate.opsForZSet().range(CACHE_SHOP_TYPE_LIST_KEY, 0, -1);
        //2.命中,转换格式返回
        if (shopSetJsonSet.size() != 0) {
            List<ShopType> shopTypes = new ArrayList<>();
            for (String jsonStr : shopSetJsonSet) {
                shopTypes.add(JSONUtil.toBean(jsonStr, ShopType.class));
            }
            return Result.ok(shopTypes);
        }
        //3.未命中,根据数据库查询
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        //4.判断商铺类型是否存在
        if (shopTypeList == null || shopTypeList.isEmpty()) {
            return Result.fail("404 商店分类不存在");
        }
        //5.不存在,返回404

        //6.存在,写入redis
        for (ShopType shopType : shopTypeList) {
            stringRedisTemplate.opsForZSet().add(CACHE_SHOP_TYPE_LIST_KEY, JSONUtil.toJsonStr(shopType), shopType.getSort());
        }
        return Result.ok(shopTypeList);
    }
}
