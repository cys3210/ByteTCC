/**
 * Copyright 2014-2016 yangming.liu<bytefox@126.com>.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, see <http://www.gnu.org/licenses/>.
 */
package org.bytesoft.bytetcc;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;

import org.bytesoft.compensable.CompensableBeanFactory;
import org.bytesoft.compensable.CompensableInvocation;
import org.bytesoft.compensable.CompensableInvocationRegistry;
import org.bytesoft.compensable.CompensableManager;
import org.bytesoft.compensable.CompensableTransaction;
import org.bytesoft.compensable.aware.CompensableBeanFactoryAware;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionManagerImpl implements TransactionManager, CompensableBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(TransactionManagerImpl.class);

	@javax.inject.Inject
	private CompensableBeanFactory beanFactory;

	// 开始全局事务
	public void begin() throws NotSupportedException, SystemException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();

		// CompensableManagerImpl
		// TCC全局事务管理器, 实现 JTA 规范的 TransactionManager 供 spring 容器直接调用,
		// 负责 spring 容器声明式事务中的 commit,rollback,suspend,resume 等管理操作
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();

		// 负责全局事务相关的处理逻辑， 实现 TransactionListener 接口，
		// 在 ByteJTA 本地事务 commit/rollback 时会收到相应的通知及
		// 获得当前线程的 TCC事务， 不支持一个线程同时开启多个 TCC事务
		CompensableTransaction transaction = compensableManager.getCompensableTransactionQuietly();

		// 所有开始调用 tcc 事务相关方法的 invocation的注册表
		CompensableInvocationRegistry registry = CompensableInvocationRegistry.getInstance();
		CompensableInvocation invocation = registry.getCurrent();

		// 若当前已经开始了 tcc 全局事务
		if (transaction != null) {
			compensableManager.begin();

		// 开始创建一个 tcc 全局事务
		// CompensableMethodInterceptor invoke, 在 tcc 事务开始之前, 拦截器拦截到被执行的方法, 开始第一次 tcc 全局事务
		// <bean id="compensableMethodInterceptor" class="org.bytesoft.bytetcc.supports.spring.CompensableMethodInterceptor" />
		// <aop:config proxy-target-class="true">
		// 	<aop:pointcut id="compensableMethodPointcut" expression="@within(org.bytesoft.compensable.Compensable)" />
		//  <aop:advisor advice-ref="compensableMethodInterceptor" pointcut-ref="compensableMethodPointcut" />
		// </aop:config>
		// aop会拦截所有标注有 compensable 注解的对象， 对于他们执行的方法都会进行拦截， 推测 invocation 主要是为了 tcc 事务的上下文
		} else if (invocation != null) {
			compensableManager.compensableBegin();

		} else {
			transactionManager.begin();
		}

	}

	// 提交全局事务
	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException,
			IllegalStateException, SystemException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();

		Transaction transaction = transactionManager.getTransactionQuietly();
		Transaction compensable = compensableManager.getCompensableTransactionQuietly();

		TransactionContext transactionContext = null;
		if (transaction == null && compensable == null) {
			throw new IllegalStateException();
		} else if (compensable == null) {
			transactionContext = transaction.getTransactionContext();
		} else {
			transactionContext = compensable.getTransactionContext();
		}

		// 判断当前事务是否为 tcc 全局事务
		if (org.bytesoft.compensable.TransactionContext.class.isInstance(transactionContext)) {
			org.bytesoft.compensable.TransactionContext compensableContext = //
					(org.bytesoft.compensable.TransactionContext) transactionContext;
			if (compensableContext.isRecoveried()) {					// 恢复事务
				if (compensableContext.isCompensable() == false) {
					throw new IllegalStateException();
				}
				compensableManager.commit();
			} else if (compensableContext.isCompensable() == false) {	// 不是全局事务
				transactionManager.commit();
			} else if (compensableContext.isCompensating()) {			// 正在补偿中的全局事务
				compensableManager.commit();
			} else if (transactionContext.isCoordinator()) {			// 本地事务已经开始了
				if (transactionContext.isPropagated()) {
					compensableManager.commit();
				} else if (compensableContext.getPropagationLevel() > 0) {
					compensableManager.commit();
				} else {
					compensableManager.compensableCommit();
				}
			} else {
				compensableManager.commit();
			}
		} else {
			transactionManager.commit();
		}

	}

	// 回滚全局事务
	public void rollback() throws IllegalStateException, SecurityException, SystemException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();

		Transaction transaction = transactionManager.getTransactionQuietly();
		Transaction compensable = compensableManager.getCompensableTransactionQuietly();

		TransactionContext transactionContext = null;
		if (transaction == null && compensable == null) {
			throw new IllegalStateException();
		} else if (compensable == null) {
			transactionContext = transaction.getTransactionContext();
		} else {
			transactionContext = compensable.getTransactionContext();
		}

		if (org.bytesoft.compensable.TransactionContext.class.isInstance(transactionContext)) {
			org.bytesoft.compensable.TransactionContext compensableContext = //
					(org.bytesoft.compensable.TransactionContext) transactionContext;
			if (compensableContext.isRecoveried()) {
				if (compensableContext.isCompensable() == false) {
					throw new IllegalStateException();
				}
				compensableManager.rollback();
			} else if (compensableContext.isCompensable() == false) {
				transactionManager.rollback();
			} else if (compensableContext.isCompensating()) {
				compensableManager.rollback();
			} else if (compensableContext.isCoordinator()) {
				if (compensableContext.isPropagated()) {
					compensableManager.rollback();
				} else if (compensableContext.getPropagationLevel() > 0) {
					compensableManager.rollback();
				} else {
					compensableManager.compensableRollback();
				}
			} else {
				compensableManager.rollback();
			}
		} else {
			transactionManager.rollback();
		}

	}

	public Transaction suspend() throws SystemException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();

		TransactionContext transactionContext = null;
		Transaction transaction = transactionManager.getTransactionQuietly();
		Transaction compensable = compensableManager.getCompensableTransactionQuietly();
		if (transaction == null && compensable == null) {
			throw new SystemException();
		} else if (compensable == null) {
			transactionContext = transaction.getTransactionContext();
		} else {
			transactionContext = compensable.getTransactionContext();
		}

		boolean isCompensableTransaction = false;
		if (org.bytesoft.compensable.TransactionContext.class.isInstance(transactionContext)) {
			org.bytesoft.compensable.TransactionContext compensableContext = //
					(org.bytesoft.compensable.TransactionContext) transactionContext;
			isCompensableTransaction = compensableContext.isCompensable();
		}
		return (isCompensableTransaction ? compensableManager : transactionManager).suspend();
	}

	public void resume(javax.transaction.Transaction tobj)
			throws InvalidTransactionException, IllegalStateException, SystemException {

		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();

		if (org.bytesoft.transaction.Transaction.class.isInstance(tobj) == false) {
			throw new InvalidTransactionException();
		}

		org.bytesoft.transaction.Transaction transaction = (org.bytesoft.transaction.Transaction) tobj;
		org.bytesoft.transaction.Transaction compensable = //
				(org.bytesoft.transaction.Transaction) transaction.getTransactionalExtra();

		if (compensable == null) {
			transactionManager.resume(tobj);
		} else {
			org.bytesoft.compensable.TransactionContext compensableContext = //
					(org.bytesoft.compensable.TransactionContext) compensable.getTransactionContext();
			(compensableContext.isCompensable() ? compensableManager : transactionManager).resume(tobj);
		}
	}

	public void setRollbackOnly() throws IllegalStateException, SystemException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();

		TransactionContext transactionContext = null;
		Transaction transaction = transactionManager.getTransactionQuietly();
		Transaction compensable = compensableManager.getCompensableTransactionQuietly();
		if (transaction == null && compensable == null) {
			throw new IllegalStateException();
		} else if (compensable == null) {
			transactionContext = transaction.getTransactionContext();
		} else {
			transactionContext = compensable.getTransactionContext();
		}

		boolean isCompensableTransaction = false;
		if (org.bytesoft.compensable.TransactionContext.class.isInstance(transactionContext)) {
			org.bytesoft.compensable.TransactionContext compensableContext = //
					(org.bytesoft.compensable.TransactionContext) transactionContext;
			isCompensableTransaction = compensableContext.isCompensable();
		}
		(isCompensableTransaction ? compensableManager : transactionManager).setRollbackOnly();
	}

	public void setTransactionTimeout(int seconds) throws SystemException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();

		TransactionContext transactionContext = null;
		Transaction transaction = transactionManager.getTransactionQuietly();
		Transaction compensable = compensableManager.getCompensableTransactionQuietly();
		if (transaction == null && compensable == null) {
			throw new IllegalStateException();
		} else if (compensable == null) {
			transactionContext = transaction.getTransactionContext();
		} else {
			transactionContext = compensable.getTransactionContext();
		}

		boolean isCompensableTransaction = false;
		if (org.bytesoft.compensable.TransactionContext.class.isInstance(transactionContext)) {
			org.bytesoft.compensable.TransactionContext compensableContext = //
					(org.bytesoft.compensable.TransactionContext) transactionContext;
			isCompensableTransaction = compensableContext.isCompensable();
		}
		(isCompensableTransaction ? compensableManager : transactionManager).setTransactionTimeout(seconds);
	}

	public Transaction getTransaction(Thread thread) {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();
		Transaction transaction = transactionManager.getTransaction(thread);
		Transaction compensable = compensableManager.getTransaction(thread);
		if (transaction != null) {
			return transaction;
		} else if (compensable != null) {
			return ((CompensableTransaction) compensable).getTransaction();
		} else {
			return null;
		}
	}

	public Transaction getTransaction() throws SystemException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		Transaction compensable = compensableManager.getCompensableTransactionQuietly();
		if (transaction != null) {
			return transaction;
		} else if (compensable != null) {
			return ((CompensableTransaction) compensable).getTransaction();
		} else {
			return null;
		}
	}

	public Transaction getTransactionQuietly() {
		try {
			return this.getTransaction();
		} catch (Exception ex) {
			return null;
		}
	}

	public int getStatus() throws SystemException {
		Transaction transaction = this.getTransaction();
		return transaction == null ? Status.STATUS_NO_TRANSACTION : transaction.getTransactionStatus();
	}

	public void setBeanFactory(CompensableBeanFactory tbf) {
		this.beanFactory = tbf;
	}

	public void associateThread(Transaction transaction) {
		throw new IllegalStateException();
	}

	public Transaction desociateThread() {
		throw new IllegalStateException();
	}

	public int getTimeoutSeconds() {
		throw new IllegalStateException();
	}

	public void setTimeoutSeconds(int timeoutSeconds) {
		throw new IllegalStateException();
	}
}
