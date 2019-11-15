package com.wangbin.clouddemo

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.{EnableAutoConfiguration, SpringBootApplication}
import org.springframework.context.annotation.ComponentScan

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan(basePackages = Array("com.wangbin.clouddemo.controller"))
class ClouddemoApplication
object ClouddemoApplication extends App{
  SpringApplication.run(Array(classOf[ClouddemoApplication]):Array[Class[_]],args)
}
