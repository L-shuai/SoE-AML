package com.soe.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.Future;

@RestController
public class EsAccess {

    @Autowired
    private ESForRule esForRule;

//    ////@Async
//    @ApiOperation("异步 有返回值")
    @GetMapping("search")
    public String searchAll() throws IOException, ParseException, InterruptedException {
        long start = System.currentTimeMillis();
//        CompletableFuture<String> createOrder = asyncService.doSomething1("create order");
//        CompletableFuture<String> reduceAccount = asyncService.doSomething2("reduce account");
//        CompletableFuture<String> saveLog = asyncService.doSomething3("save log");
//
//        // 等待所有任务都执行完
//        CompletableFuture.allOf(createOrder, reduceAccount, saveLog).join();
//        // 获取每个任务的返回结果
//        String result = createOrder.get() + reduceAccount.get() + saveLog.get();
//        return result;
        Future<String> task1 = esForRule.rule_1();
        Future<String> task2 = esForRule.rule_2();
        Future<String> task3 = esForRule.rule_3();
        Future<String> task4 = esForRule.rule_4();
        Future<String> task5 = esForRule.rule_5();
        Future<String> task6 = esForRule.rule_6();
        Future<String> task7 = esForRule.rule_7();
        Future<String> task8 = esForRule.rule_8();
        Future<String> task9 = esForRule.rule_9();
        Future<String> task10 = esForRule.rule_10();
        Future<String> task11 = esForRule.rule_11();
        Future<String> task12 = esForRule.rule_12();
        Future<String> task13 = esForRule.rule_13();
        Future<String> task14 = esForRule.rule_14();
        Future<String> task15 = esForRule.rule_15_new();
        Future<String> task16 = esForRule.rule_16();
        Future<String> task17 = esForRule.rule_17();
        Future<String> task18 = esForRule.rule_18();
        Future<String> task19 = esForRule.rule_19();
        Future<String> task20 = esForRule.rule_20();
//        while(true) {
//            if(task1.isDone() && task2.isDone() && task3.isDone()) {
//                // 三个任务都调用完成，退出循环等待
//                break;
//            }
//            Thread.sleep(1000);
//        }
        while(true){
            if(task1.isDone() && task2.isDone() && task3.isDone() && task4.isDone() &&
                    task5.isDone() && task6.isDone() && task7.isDone() && task8.isDone() &&
                    task9.isDone() && task10.isDone() && task11.isDone() && task12.isDone() &&
                    task13.isDone() && task14.isDone() && task15.isDone() && task16.isDone() &&
                    task17.isDone() && task18.isDone() && task19.isDone() && task20.isDone()) {
                // 20个任务都调用完成，退出循环等待
                break;
            }
            Thread.sleep(1000);
        }
        long end = System.currentTimeMillis();
        long use_times = (end - start)/60000;
        String s = "任务全部完成，总耗时：" + use_times + "分钟";

        return s;

    }
}
