package com.homestead.utils;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * @author YangHanBin
 */
@Component
public class SpringBeanFactory implements BeanFactoryAware {

	private static BeanFactory beanFactory;

	@Override
	public void setBeanFactory(@Nullable BeanFactory factory) throws BeansException {
		beanFactory = factory;
	}

	public static Object getBean(String name) {
		return beanFactory.getBean(name);
	}

	public static <T> T getBean(String name , Class<T> requiredType) {
		return beanFactory.getBean(name, requiredType);
	}

	public static <T> T getBean(Class<T> requiredType) {
		return beanFactory.getBean(requiredType);
	}
}

