/**
 *    Copyright 2009-2019 the original author or authors.
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

import static org.apache.ibatis.executor.ExecutionPlaceholder.EXECUTION_PLACEHOLDER;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.statement.StatementUtil;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.logging.jdbc.ConnectionLogger;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.type.TypeHandlerRegistry;

/**
 * @author Clinton Begin
 */
public abstract class BaseExecutor implements Executor {

  private static final Log log = LogFactory.getLog(BaseExecutor.class);

  protected Transaction transaction;  //Transaction对象，实现事务的提交、回滚和关闭操作
  protected Executor wrapper; //封装了真正的 Executor对象
  //定义线程安全队列，此类继承和实现如下
  /*public class ConcurrentLinkedQueue<E> extends AbstractQueue<E>
        implements Queue<E>, java.io.Serializable {
  延迟加载队列。一个基于链接节点的无界线程安全队列。此队列按照 FIFO（先进先出）原则对元素进行排序。
  当多个线程共享访问一个公共 collection 时，ConcurrentLinkedQueue 是一个恰当的选择。此队列不允许使用 null元素。该变量主要是存储一些可以延时加载的变量对象，且可供多线程使用*/
  protected ConcurrentLinkedQueue<DeferredLoad> deferredLoads;
  //一级缓存，用于缓存该 Executor对象查询结果集映射得到的结果对象，此类中成员如下
  /*public class PerpetualCache implements Cache {
  private final String id;
  private Map<Object, Object> cache = new HashMap<>();
  */
  protected PerpetualCache localCache;
  protected PerpetualCache localOutputParameterCache; //一级缓存，用于缓存输出类型的参数
  protected Configuration configuration;  //mybatis的配置信息,全局唯一配置对象

  protected int queryStack;  //查询的深度，用来记录嵌套查询的层数，分析 DefaultResultSetHandler时介绍过的嵌套查询
  private boolean closed;   //标识该执行器是否已经关闭

  protected BaseExecutor(Configuration configuration, Transaction transaction) {
    this.transaction = transaction;
    this.deferredLoads = new ConcurrentLinkedQueue<>();
    this.localCache = new PerpetualCache("LocalCache");
    this.localOutputParameterCache = new PerpetualCache("LocalOutputParameterCache");
    this.closed = false;
    this.configuration = configuration; //主文件属性，主要获取MappedStatement对象
    this.wrapper = this;
  }

  @Override
  public Transaction getTransaction() {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    return transaction;
  }

  /**
   * 关闭当前执行器，如果需要回滚，就执行回滚操作
   * 需要把相应的资源（比如事务transaction等），同步关闭
   * 把其他变量设置成NULL
   */
  @Override
  public void close(boolean forceRollback) {
    try {
      try {
        rollback(forceRollback);
      } finally {
        if (transaction != null) {
          transaction.close();
        }
      }
    } catch (SQLException e) {
      // Ignore.  There's nothing that can be done at this point.
      log.warn("Unexpected exception on closing transaction.  Cause: " + e);
    } finally {
      transaction = null;
      deferredLoads = null;
      localCache = null;
      localOutputParameterCache = null;
      closed = true;
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  /**
   * update/insert/delete请求到达SqlSession都会调用此方法
    从语义的角度，insert、update、delete都是属于对数据库的行进行更新操作
    从实现的角度，我们熟悉的PreparedStatement里面提供了两种execute方法，一种是executeUpdate()，
    一种是executeQuery()，前者对应的是insert、update与delete，后者对应的是select，因此对于MyBatis来说只有update与select
    该方法处理了通用的情况，真正实现由子类通过实现doUpdate方法完成。该方法实现了：
    判断该执行器是否已经关闭，如果关闭，就直接抛出异常
    因为该操作是更新操作，所以该操作会让其中的缓存就全部失效，即清理缓存
    调用抽象方法doUpdate实现数据更新，由子类完成。
    */
  @Override
  public int update(MappedStatement ms, Object parameter) throws SQLException {
    ErrorContext.instance().resource(ms.getResource()).activity("executing an update").object(ms.getId());
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    clearLocalCache();
    return doUpdate(ms, parameter); //调用抽象方法doUpdate实现数据更新，如何更新由子类实现，这里利用模板方法模式
  }

  /**
   * 刷新Statement。真正的实现由具体的子类完成，且在不同的实现类中的逻辑不一样
   */
  @Override
  public List<BatchResult> flushStatements() throws SQLException {
    return flushStatements(false);
  }

  public List<BatchResult> flushStatements(boolean isRollBack) throws SQLException {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    return doFlushStatements(isRollBack);
  }

  @Override
  public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler) throws SQLException {
    BoundSql boundSql = ms.getBoundSql(parameter);  //得到绑定sql，并将参数对象与sql语句的#{}一一对应
    CacheKey key = createCacheKey(ms, parameter, rowBounds, boundSql);   //创建获取cacheKey供缓存，包含完整的语句、参数等，确保CacheKey的唯一性
    return query(ms, parameter, rowBounds, resultHandler, key, boundSql);
  }
  /**
   * SqlSession.selectList会调用此方法
   在该方法中，首先判断当前执行器是否已经关闭；然后再根据queryStack查询层数和flushCache属性判断，
   是否需要清除一级缓存；然后再判断结果处理器resultHandler是否为空，为空的话，尝试从缓存中查询数据，
   否则直接为空，后续从数据库查询数据；然后再根据缓存中是否有数据，如果存在数据，
   且是存储过程或函数类型则执行handleLocallyCachedOutputParameters()方法，如果存在数据，
   不是存储过程或函数类型就直接返回，如果缓存中没有数据就直接通过queryFromDatabase()方法从数据库查询数据，
   其中又通过doQuery()方法实现查询逻辑；后续在通过DeferredLoad类实现嵌套查询的延时加载功能
   */
  @SuppressWarnings("unchecked")
  @Override
  public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, CacheKey key, BoundSql boundSql) throws SQLException {
    ErrorContext.instance().resource(ms.getResource()).activity("executing a query").object(ms.getId());
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    if (queryStack == 0 && ms.isFlushCacheRequired()) {
      clearLocalCache();
    }
    List<E> list;
    try {
      queryStack++; //本地缓存记录自增，这样递归调用到上面的时候就不会再清局部缓存了
      list = resultHandler == null ? (List<E>) localCache.getObject(key) : null;  //如果查询的语句已存在本地缓存中，则直接从本地获取，反之从数据库中读取内容
      if (list != null) {
        /*针对存储过程调用的处理 其功能是 在一级缓存命中时，获取缓存中保存的输出类型参数，
    	并设到用户传入的实参（ parameter ）对象中。
        */
        handleLocallyCachedOutputParameters(ms, key, parameter, boundSql);
      } else {
        //从数据库中获取并进行缓存处理，其也会调用子类需复写的doQuery()方法
        list = queryFromDatabase(ms, parameter, rowBounds, resultHandler, key, boundSql);
      }
    } finally {
      queryStack--; //清空堆栈操作
    }
    if (queryStack == 0) {
      for (DeferredLoad deferredLoad : deferredLoads) { //延迟加载队列中所有元素
        deferredLoad.load();
      }
      // issue #601
      deferredLoads.clear();  //清空延迟加载队列
      /**
       * LocalCacheScope为STATEMENT的时候，会清理缓存。但是结果 list已经从localcache拿到了，然后直接返回了。再清理缓存，还有啥意义？
       * 这个问题不应该割裂的看，应该从第一次查询来看。
       * 第一次查询的时候，list必然为空，然后从db查询出结果放到localCache。
       * 放完后，发现是statement级别，需要再清理掉缓存，那么cache又变成了空。
       * 第二次查询的时候，虽然优先从缓存中取，但是照样取不到。所以，就没有上面的list拿到结果直接返回给用户的问题了！！！
       */
      if (configuration.getLocalCacheScope() == LocalCacheScope.STATEMENT) {
        // issue #482
        clearLocalCache();  //如果是statement，清本地缓存
      }
    }
    return list;
  }

  @Override
  public <E> Cursor<E> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) throws SQLException {
    BoundSql boundSql = ms.getBoundSql(parameter);
    return doQueryCursor(ms, parameter, rowBounds, boundSql);
  }

  @Override
  public void deferLoad(MappedStatement ms, MetaObject resultObject, String property, CacheKey key, Class<?> targetType) {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    DeferredLoad deferredLoad = new DeferredLoad(resultObject, property, key, localCache, configuration, targetType);
    if (deferredLoad.canLoad()) {
      deferredLoad.load();
    } else {
      deferredLoads.add(new DeferredLoad(resultObject, property, key, localCache, configuration, targetType));
    }
  }
  /**
     createCacheKey()方法
     创建缓存中使用的key，即CacheKey实例对象。创建的CacheKey实例对象，由下列参数决定其唯一性，
     即相同的查询，相关的参数，生成的CacheKey实例唯一且不变。依据的参数如下：
     1、MappedStatement实例的ID
     2、RowBounds实例的offset和limit属性
     3、BoundSql实例的sql属性
     4、查询条件的参数值
     5、连接数据库的环境ID
     MappedStatement（一般为xml中定义）、parameterObject（传递给xml的参数）、
     rowBounds（分页参数）、boundSql（最终sql，由MappedStatement和parameterObject决定）
      */
    //还有如果一个查询的 id、分页组件中的 offset 和 limit、sql 语句、参数 都保持不变，
    //那么这个查询产生的 CacheKey一定是不变的。在一个 SqlSession 的生命周期内，二次同样的查询 CacheKey 是一样的
  @Override
  public CacheKey createCacheKey(MappedStatement ms, Object parameterObject, RowBounds rowBounds, BoundSql boundSql) {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    CacheKey cacheKey = new CacheKey();
    cacheKey.update(ms.getId());
    cacheKey.update(rowBounds.getOffset());
    cacheKey.update(rowBounds.getLimit());
    cacheKey.update(boundSql.getSql());
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    TypeHandlerRegistry typeHandlerRegistry = ms.getConfiguration().getTypeHandlerRegistry();
    // mimic DefaultParameterHandler logic
    for (ParameterMapping parameterMapping : parameterMappings) {
      if (parameterMapping.getMode() != ParameterMode.OUT) {
        Object value;
        String propertyName = parameterMapping.getProperty();
        if (boundSql.hasAdditionalParameter(propertyName)) {
          value = boundSql.getAdditionalParameter(propertyName);
        } else if (parameterObject == null) {
          value = null;
        } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
          value = parameterObject;
        } else {
          MetaObject metaObject = configuration.newMetaObject(parameterObject);
          value = metaObject.getValue(propertyName);
        }
        cacheKey.update(value);
      }
    }
    if (configuration.getEnvironment() != null) {
      // issue #176
      cacheKey.update(configuration.getEnvironment().getId());
    }
    return cacheKey;
  }

  @Override
  public boolean isCached(MappedStatement ms, CacheKey key) {
    return localCache.getObject(key) != null;
  }

  @Override
  public void commit(boolean required) throws SQLException {
    if (closed) {
      throw new ExecutorException("Cannot commit, transaction is already closed");
    }
    clearLocalCache();
    flushStatements();
    if (required) {
      transaction.commit();
    }
  }

  @Override
  public void rollback(boolean required) throws SQLException {
    if (!closed) {
      try {
        clearLocalCache();
        flushStatements(true);
      } finally {
        if (required) {
          transaction.rollback();
        }
      }
    }
  }

  @Override
  public void clearLocalCache() {
    if (!closed) {
      localCache.clear();
      localOutputParameterCache.clear();
    }
  }

  protected abstract int doUpdate(MappedStatement ms, Object parameter)
      throws SQLException;

  protected abstract List<BatchResult> doFlushStatements(boolean isRollback)
      throws SQLException;

  protected abstract <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql)
      throws SQLException;

  protected abstract <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds, BoundSql boundSql)
      throws SQLException;

  protected void closeStatement(Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  /**
   * Apply a transaction timeout.
   * @param statement a current statement
   * @throws SQLException if a database access error occurs, this method is called on a closed <code>Statement</code>
   * @since 3.4.0
   * @see StatementUtil#applyTransactionTimeout(Statement, Integer, Integer)
   */
  protected void applyTransactionTimeout(Statement statement) throws SQLException {
    StatementUtil.applyTransactionTimeout(statement, statement.getQueryTimeout(), transaction.getTimeout());
  }

  private void handleLocallyCachedOutputParameters(MappedStatement ms, CacheKey key, Object parameter, BoundSql boundSql) {
    if (ms.getStatementType() == StatementType.CALLABLE) {
      final Object cachedParameter = localOutputParameterCache.getObject(key);
      if (cachedParameter != null && parameter != null) {
        final MetaObject metaCachedParameter = configuration.newMetaObject(cachedParameter);
        final MetaObject metaParameter = configuration.newMetaObject(parameter);
        for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
          if (parameterMapping.getMode() != ParameterMode.IN) {
            final String parameterName = parameterMapping.getProperty();
            final Object cachedValue = metaCachedParameter.getValue(parameterName);
            metaParameter.setValue(parameterName, cachedValue);
          }
        }
      }
    }
  }

  private <E> List<E> queryFromDatabase(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, CacheKey key, BoundSql boundSql) throws SQLException {
    List<E> list;
    localCache.putObject(key, EXECUTION_PLACEHOLDER); //用于解决子查询中的循环依赖
    try {
      list = doQuery(ms, parameter, rowBounds, resultHandler, boundSql);
    } finally {
      localCache.removeObject(key);
    }
    localCache.putObject(key, list);
    if (ms.getStatementType() == StatementType.CALLABLE) {
      localOutputParameterCache.putObject(key, parameter);
    }
    return list;
  }

  protected Connection getConnection(Log statementLog) throws SQLException {
    Connection connection = transaction.getConnection();
    if (statementLog.isDebugEnabled()) {
      return ConnectionLogger.newInstance(connection, statementLog, queryStack);
    } else {
      return connection;
    }
  }

  @Override
  public void setExecutorWrapper(Executor wrapper) {
    this.wrapper = wrapper;
  }

  private static class DeferredLoad {

    private final MetaObject resultObject;
    private final String property;
    private final Class<?> targetType;
    private final CacheKey key;
    private final PerpetualCache localCache;
    private final ObjectFactory objectFactory;
    private final ResultExtractor resultExtractor;

    // issue #781
    public DeferredLoad(MetaObject resultObject,
                        String property,
                        CacheKey key,
                        PerpetualCache localCache,
                        Configuration configuration,
                        Class<?> targetType) {
      this.resultObject = resultObject;
      this.property = property;
      this.key = key;
      this.localCache = localCache;
      this.objectFactory = configuration.getObjectFactory();
      this.resultExtractor = new ResultExtractor(configuration, objectFactory);
      this.targetType = targetType;
    }

    public boolean canLoad() {
      return localCache.getObject(key) != null && localCache.getObject(key) != EXECUTION_PLACEHOLDER;
    }

    public void load() {
      @SuppressWarnings("unchecked")
      // we suppose we get back a List
      List<Object> list = (List<Object>) localCache.getObject(key);
      Object value = resultExtractor.extractObjectFromList(list, targetType);
      resultObject.setValue(property, value);
    }

  }

}
