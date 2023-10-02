/*
 *    Copyright 2009-2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.executor;

/**
 * @author Clinton Begin
 */
public class ErrorContext {
  /**
   * 获取操作系统对应的换行符，各个操作系统不同
   * windows下的文本文件换行符:\r\n
   * linux/unix下的文本文件换行符:\r
   * Mac下的文本文件换行符:\n
   *
   * 也是换行符,功能和"\n"是一致的,但是此种写法屏蔽了 Windows和Linux的区别 ，更保险一些
   */
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private static final ThreadLocal<ErrorContext> LOCAL = ThreadLocal.withInitial(ErrorContext::new);

  private ErrorContext stored;  //链式
  private String resource;      //资源文件
  private String activity;      //行为（增删改查等）
  private String object;        //异常发生在哪个对象
  private String message;       //异常消息的内容
  private String sql;           //哪条sql发生异常
  private Throwable cause;      //异常栈

  private ErrorContext() {
  }

  public static ErrorContext instance() {
    return LOCAL.get();
  }

  public ErrorContext store() {
    ErrorContext newContext = new ErrorContext(); //new一个新的ErrorContext
    newContext.stored = this;   //把当前的ErrorContext作为“旧的”存到stored
    LOCAL.set(newContext);
    return LOCAL.get();
  }

  public ErrorContext recall() {
    if (stored != null) {
      LOCAL.set(stored);
      stored = null;
    }
    return LOCAL.get();
  }

  public ErrorContext resource(String resource) {
    this.resource = resource;
    return this;
  }

  public ErrorContext activity(String activity) {
    this.activity = activity;
    return this;
  }

  public ErrorContext object(String object) {
    this.object = object;
    return this;
  }

  public ErrorContext message(String message) {
    this.message = message;
    return this;
  }

  public ErrorContext sql(String sql) {
    this.sql = sql;
    return this;
  }

  public ErrorContext cause(Throwable cause) {
    this.cause = cause;
    return this;
  }

  public ErrorContext reset() {
    resource = null;
    activity = null;
    object = null;
    message = null;
    sql = null;
    cause = null;
    LOCAL.remove();
    return this;
  }

  @Override
  public String toString() {
    StringBuilder description = new StringBuilder();

    // message
    if (this.message != null) {
      description.append(LINE_SEPARATOR);
      description.append("### ");
      description.append(this.message);
    }

    // resource
    if (resource != null) {
      description.append(LINE_SEPARATOR);
      description.append("### The error may exist in ");
      description.append(resource);
    }

    // object
    if (object != null) {
      description.append(LINE_SEPARATOR);
      description.append("### The error may involve ");
      description.append(object);
    }

    // activity
    if (activity != null) {
      description.append(LINE_SEPARATOR);
      description.append("### The error occurred while ");
      description.append(activity);
    }

    // sql
    if (sql != null) {
      description.append(LINE_SEPARATOR);
      description.append("### SQL: ");
      description.append(sql.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').trim());
    }

    // cause
    if (cause != null) {
      description.append(LINE_SEPARATOR);
      description.append("### Cause: ");
      description.append(cause.toString());
    }

    return description.toString();
  }

}
