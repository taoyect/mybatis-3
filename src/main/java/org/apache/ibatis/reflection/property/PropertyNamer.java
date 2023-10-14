/*
 *    Copyright 2009-2023 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.reflection.property;

import java.util.Locale;

import org.apache.ibatis.reflection.ReflectionException;

/**
 * @author Clinton Begin 转换方法名到属性名，以及检测一个方法名是否为 getter 或 setter 方法
 */
public final class PropertyNamer {

  private PropertyNamer() {
    // Prevent Instantiation of Static Class
  }

  public static String methodToProperty(String name) {
    if (name.startsWith("is")) {
      name = name.substring(2);
    } else if (name.startsWith("get") || name.startsWith("set")) {
      name = name.substring(3);
    } else {
      throw new ReflectionException(
          "Error parsing property name '" + name + "'.  Didn't start with 'is', 'get' or 'set'.");
    }
    // name.length() == 1 --> 对于属性只有一个字母的，例如private int x;
    // name.length() > 1 && !Character.isUpperCase(name.charAt(1))) --> 属性名字长度大于1，并且第二个字母是小写的
    // 所以此处：如果属性名字长度大于1且第二个字母是大写的，将不会处理第一个字母
    if (name.length() == 1 || name.length() > 1 && !Character.isUpperCase(name.charAt(1))) {
      // 让属性名第一个字母小写，然后加上后面的内容
      name = name.substring(0, 1).toLowerCase(Locale.ENGLISH) + name.substring(1);
    }

    return name;
  }

  public static boolean isProperty(String name) {
    return isGetter(name) || isSetter(name);
  }

  public static boolean isGetter(String name) {
    return name.startsWith("get") && name.length() > 3 || name.startsWith("is") && name.length() > 2;
  }

  public static boolean isSetter(String name) {
    return name.startsWith("set") && name.length() > 3;
  }

}
