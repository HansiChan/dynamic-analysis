package com.dachen.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.ParameterBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.schema.ModelRef;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Parameter;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by test on 2017/6/7.
 */
@Configuration
@EnableSwagger2
public class Swagger2 {

    @Bean
    public Docket createRestApi() {
         ParameterBuilder tokenPar = new ParameterBuilder();  
         List<Parameter> pars = new ArrayList<Parameter>();  
         tokenPar.name("access-token")
         		.description("令牌")
         		.modelRef(new ModelRef("string"))
         		.parameterType("header")
         		.required(true)
         		.build();  
         pars.add(tokenPar.build());  
         
        return new Docket(DocumentationType.SWAGGER_2)
            .apiInfo(apiInfo())
            .globalOperationParameters(pars)
            .select()
            .apis(RequestHandlerSelectors.basePackage("com.dachen"))
            .paths(PathSelectors.any())
            .build();
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
            .title("用户动态分析")
            .description("")
            .termsOfServiceUrl("")
            .version("1.0")
            .description("访问方式：http://{domain}/content-recommend/xxxxx(接口访问默认都是要带token的，userID是内部测试用的，前端根据token请求不需要传，后台自己后去取token对应的userID)")
            .build();
    }
}
