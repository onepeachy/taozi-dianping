package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisWorker redisWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private IVoucherOrderService proxy;

    public static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    //获取pending list中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断信息获取是否成功
                    if (list == null || list.isEmpty()) {
                        break;
                    }
                    //解析订单信息
                    MapRecord<String, Object, Object> mapRecord = list.get(0);
                    Map<Object, Object> map = mapRecord.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(map, new VoucherOrder(), true);
                    //获取成功,可以下单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", mapRecord.getId());
                } catch (Exception e) {
                    log.error("处理订单异常！");
                }
            }
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
//        2.查询优惠券信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //3.判断秒杀
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //3.1 没开始
            return Result.fail("秒杀尚未开始!");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            //3.2 结束了
            return Result.fail("秒杀已经结束啦!");
        }

        //执行lua脚本
        long orderId = redisWorker.nextId("order");
        Long userId = UserHolder.getUser().getId();
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //有购买资格
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);

    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean islock = lock.tryLock();
        if (!islock) {
            log.error("不允许重复下单");
            return;
        }


        proxy.createVoucherOrder(voucherOrder);

    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.提交优惠券id
//        //2.查询优惠券信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //3.判断秒杀
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            //3.1 没开始
//            return Result.fail("秒杀尚未开始!");
//        }
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            //3.2 结束了
//            return Result.fail("秒杀已经结束啦!");
//        }
//        //4.在秒杀时间范围内,判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足!");
//        }
//
//        //返回
//        Long userId = UserHolder.getUser().getId();
//
//        //获取锁
////        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
//        RLock lock = redissonClient.getLock("order:" + userId);
//        boolean isLock = lock.tryLock();
//        if (!isLock) {
//            return Result.fail("不能多次下单!");
//        }
//        try {
//            //获取代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//    }

    @Transactional
    public Result createVoucherOrder(VoucherOrder voucherOrder) {
        //5.充足,扣减库存,创建订单
        Long voucherId = voucherOrder.getVoucherId();
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();

        if (!success) {
            return Result.fail("创建订单失败");
        }

        save(voucherOrder);

        return Result.ok(voucherOrder.getId());
    }
}
