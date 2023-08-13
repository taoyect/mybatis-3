/**
 *    Copyright 2009-2017 the original author or authors.
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
package org.apache.ibatis.reflection.property;

import java.util.Iterator;

/**
 * @author Clinton Begin
 * https://blog.csdn.net/seasonsbin/article/details/116998082
 *
 * 属性分词器
 *
 *  fullname: "name"
 *    name:"name"
 *    indexedName:"name"
 *    index:null
 *    children:null
 *
 *  fullname: "student.name"
 *    name:"student"
 *    indexedName:"student"
 *    index:null
 *    children:"name"
 *
 *  fullname: "students[0]"
 *    name:"students"
 *    indexedName:"students[0]"
 *    index:"0"
 *    children:null
 *
 *  fullname: "claim.owners[0].ownerName"
 *  PropertyTokenizer first = new PropertyTokenizer(fullname)
 *    name:"claim"
 *    indexedName:"claim"
 *    index:null
 *    children:"owners[0].ownerName"
 *  PropertyTokenizer second = new PropertyTokenizer(children)
 *    name:"owners"
 *    indexedName:"owners[0]"
 *    index:"0"
 *    children:"ownerName"
 *
 */
public class PropertyTokenizer implements Iterator<PropertyTokenizer> {
  private String name;  //父表达式，一般对应被操作Bean的fieldName,根据该name可以获取被操作bean的属性的getter/setter
  //带索引的表达式，由父表达式和下标组成，indexedName一般应用于带下标的表达式。一些bean的属性类型可能是array|list，
  // 对于array|list，我们必须知道下标才能操作其中的元素，indexedName负责保存这类属性的字段名称和下标
  private final String indexedName;
  private String index; //下标索引
  private final String children;

  public PropertyTokenizer(String fullname) {
    int delim = fullname.indexOf('.');
    if (delim > -1) {
      name = fullname.substring(0, delim);
      children = fullname.substring(delim + 1);
    } else {
      name = fullname;
      children = null;
    }
    indexedName = name;
    delim = name.indexOf('[');
    if (delim > -1) {
      index = name.substring(delim + 1, name.length() - 1);
      name = name.substring(0, delim);
    }
  }

  public String getName() {
    return name;
  }

  public String getIndex() {
    return index;
  }

  public String getIndexedName() {
    return indexedName;
  }

  public String getChildren() {
    return children;
  }

  @Override
  public boolean hasNext() {
    return children != null;
  }

  @Override
  public PropertyTokenizer next() {
    return new PropertyTokenizer(children);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove is not supported, as it has no meaning in the context of properties.");
  }
}
