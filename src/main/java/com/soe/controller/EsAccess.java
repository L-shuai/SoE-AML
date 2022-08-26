package com.soe.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.text.ParseException;

@RestController
public class EsAccess {

    @Autowired
    private ESForRule esForRule;

    @Async
//    @ApiOperation("异步 有返回值")
    @GetMapping("search")
    public void searchAll() throws IOException, ParseException {
//        CompletableFuture<String> createOrder = asyncService.doSomething1("create order");
//        CompletableFuture<String> reduceAccount = asyncService.doSomething2("reduce account");
//        CompletableFuture<String> saveLog = asyncService.doSomething3("save log");
//
//        // 等待所有任务都执行完
//        CompletableFuture.allOf(createOrder, reduceAccount, saveLog).join();
//        // 获取每个任务的返回结果
//        String result = createOrder.get() + reduceAccount.get() + saveLog.get();
//        return result;
        esForRule.rule_1();
        esForRule.rule_2();
        esForRule.rule_3();
        esForRule.rule_4();
        esForRule.rule_6();
        esForRule.rule_7();
        esForRule.rule_8();
        esForRule.rule_9();
        esForRule.rule_10();
        esForRule.rule_12();
        esForRule.rule_15();
        esForRule.rule_16();
        esForRule.rule_17();
        esForRule.rule_20();

    }
}
