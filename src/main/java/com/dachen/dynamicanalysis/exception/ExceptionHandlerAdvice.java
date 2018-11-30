package com.dachen.dynamicanalysis.exception;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.BindException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;


@ControllerAdvice
public class ExceptionHandlerAdvice {
	private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionHandlerAdvice.class);

	@ExceptionHandler(value = { ServiceException.class, IllegalArgumentException.class })
	public void handleErrors(HttpServletRequest request, HttpServletResponse response, Exception e) throws Exception {
		LOGGER.error("接口请求错误：{}",request.getRequestURI());
		LOGGER.error("错误内容：{}",(Object)e.getStackTrace());
		LOGGER.error("错误信息：{}",e.getMessage());
		e.printStackTrace();

		int resultCode = 100100;
		String resultMsg = "服务器繁忙，请稍后再试！";
		String detailMsg = "";

		if (e instanceof MissingServletRequestParameterException || e instanceof BindException) {
			resultCode = 100101;
			resultMsg = "请求参数验证失败，缺少必填参数或参数错误";
		} else if (e instanceof ServiceException) {
			ServiceException ex = ((ServiceException) e);

			resultCode = null == ex.getResultCode() ? 0 : ex.getResultCode();
			resultMsg = ex.getMessage();
		} else {
			detailMsg = e.getMessage();
		}

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("resultCode", resultCode);
		map.put("resultMsg", resultMsg);
		map.put("detailMsg", detailMsg);

		String text = JSON.toJSONString(map);

		response.setContentType("application/json; charset=UTF-8");
		response.setCharacterEncoding("UTF-8");
		response.getWriter().write(text);
		response.getWriter().flush();
	}


}
