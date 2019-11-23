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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.bytetcc.supports.CompensableSynchronization;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.compensable.CompensableBeanFactory;
import org.bytesoft.compensable.CompensableManager;
import org.bytesoft.compensable.CompensableTransaction;
import org.bytesoft.compensable.TransactionContext;
import org.bytesoft.compensable.archive.CompensableArchive;
import org.bytesoft.compensable.aware.CompensableBeanFactoryAware;
import org.bytesoft.compensable.aware.CompensableEndpointAware;
import org.bytesoft.compensable.logging.CompensableLogger;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionLock;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompensableManagerImpl implements CompensableManager, CompensableBeanFactoryAware, CompensableEndpointAware {
	static final Logger logger = LoggerFactory.getLogger(CompensableManagerImpl.class);

	@javax.inject.Inject
	private CompensableBeanFactory beanFactory;
	private String endpoint;

	private final Map<Thread, Transaction> compensableMap = new ConcurrentHashMap<Thread, Transaction>();

	public void associateThread(Transaction transaction) {
		this.compensableMap.put(Thread.currentThread(), (CompensableTransaction) transaction);
	}

	public CompensableTransaction desociateThread() {
		return (CompensableTransaction) this.compensableMap.remove(Thread.currentThread());
	}

	public int getStatus() throws SystemException {
		Transaction transaction = this.getTransactionQuietly();
		return transaction == null ? Status.STATUS_NO_TRANSACTION : transaction.getStatus();
	}

	public Transaction getTransaction(Thread thread) {
		CompensableTransaction transaction = this.getCompensableTransaction(thread);
		return transaction == null ? null : transaction.getTransaction();
	}

	public Transaction getTransactionQuietly() {
		CompensableTransaction transaction = this.getCompensableTransactionQuietly();
		return transaction == null ? null : transaction.getTransaction();
	}

	public Transaction getTransaction() throws SystemException {
		CompensableTransaction transaction = this.getCompensableTransactionQuietly();
		return transaction == null ? null : transaction.getTransaction();
	}

	public CompensableTransaction getCompensableTransactionQuietly() {
		return (CompensableTransaction) this.compensableMap.get(Thread.currentThread());
	}

	public CompensableTransaction getCompensableTransaction(Thread thread) {
		return (CompensableTransaction) this.compensableMap.get(thread);
	}

	public void resume(javax.transaction.Transaction tobj)
			throws InvalidTransactionException, IllegalStateException, SystemException {
		if (Transaction.class.isInstance(tobj)) {
			TransactionManager transactionManager = this.beanFactory.getTransactionManager();
			Transaction transaction = (Transaction) tobj;
			CompensableTransaction compensable = (CompensableTransaction) transaction.getTransactionalExtra();

			compensable.setTransactionalExtra(transaction);
			compensable.resume();

			TransactionContext compensableContext = compensable.getTransactionContext();
			compensableContext.setPropagationLevel(compensableContext.getPropagationLevel() - 1);

			transactionManager.resume(transaction);
		} else {
			throw new InvalidTransactionException();
		}
	}

	public Transaction suspend() throws SystemException {
		CompensableTransaction compensable = (CompensableTransaction) this.compensableMap.get(Thread.currentThread());
		if (compensable == null) {
			throw new SystemException();
		}

		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		Transaction transaction = transactionManager.suspend();

		TransactionContext compensableContext = compensable.getTransactionContext();
		compensableContext.setPropagationLevel(compensableContext.getPropagationLevel() + 1);

		compensable.suspend();
		compensable.setTransactionalExtra(null);

		return transaction;
	}

	public void begin() throws NotSupportedException, SystemException {
		XidFactory transactionXidFactory = this.beanFactory.getTransactionXidFactory();
		CompensableTransaction compensable = this.getCompensableTransactionQuietly();
		if (compensable == null || compensable.getTransaction() != null) {
			throw new SystemException();
		}

		CompensableArchive archive = compensable.getCompensableArchive();

		// The current confirm/cancel operation has been assigned an xid.
		TransactionXid compensableXid = archive == null ? null : (TransactionXid) archive.getCompensableXid();
		TransactionXid transactionXid = compensableXid != null //
				? transactionXidFactory.createGlobalXid(compensableXid.getGlobalTransactionId())
				: transactionXidFactory.createGlobalXid();

		TransactionContext compensableContext = compensable.getTransactionContext();
		TransactionContext transactionContext = compensableContext.clone();
		transactionContext.setXid(transactionXid);

		this.invokeBegin(transactionContext, false);
	}

	protected void invokeBegin(TransactionContext transactionContext, boolean createFlag)
			throws NotSupportedException, SystemException {
		// jta 事务协助者
		RemoteCoordinator transactionCoordinator = this.beanFactory.getTransactionCoordinator();

		CompensableTransaction compensable = this.getCompensableTransactionQuietly();
		TransactionContext compensableContext = compensable.getTransactionContext();

		// tcc 事务id
		TransactionXid compensableXid = compensableContext.getXid();
		// jta 事务id
		TransactionXid transactionXid = transactionContext.getXid();
		try {
			// 开始 jta 本地事务(创建一个 2PC 机制的事务)
			// bytejta 是基于 XA/2PC 机制的事务管理器, 此处基于 bytejta 管理本地事务
			Transaction transaction = transactionCoordinator.start(transactionContext, XAResource.TMNOFLAGS);
			transaction.setTransactionalExtra(compensable);
			compensable.setTransactionalExtra(transaction);

			// 将 tcc 事务注册到 jta 事务中
			// 此时 jta 事务在进行 prepare, commit ,rollback 等操作时都会通知 tcc 全局事务
			transaction.registerTransactionResourceListener(compensable);
			transaction.registerTransactionListener(compensable);

			// 本地事务 - 分支事务 同步
			CompensableSynchronization synchronization = this.beanFactory.getCompensableSynchronization();
			synchronization.afterBegin(compensable.getTransaction(), createFlag);
		} catch (XAException tex) {
			logger.info("[{}] begin-transaction: error occurred while starting jta-transaction: {}",
					ByteUtils.byteArrayToString(compensableXid.getGlobalTransactionId()),
					ByteUtils.byteArrayToString(transactionXid.getGlobalTransactionId()), tex);
			try {
				transactionCoordinator.end(transactionContext, XAResource.TMFAIL);
				throw new SystemException("Error occurred while beginning a compensable-transaction!");
			} catch (XAException ignore) {
				throw new SystemException("Error occurred while beginning a compensable-transaction!");
			}
		}
	}

	protected void invokeRollbackInBegin(TransactionContext transactionContext) throws NotSupportedException, SystemException {
		RemoteCoordinator transactionCoordinator = this.beanFactory.getTransactionCoordinator();

		CompensableTransaction compensable = this.getCompensableTransactionQuietly();
		TransactionContext compensableContext = compensable.getTransactionContext();

		TransactionXid compensableXid = compensableContext.getXid();
		TransactionXid transactionXid = transactionContext.getXid();
		try {
			transactionCoordinator.end(transactionContext, XAResource.TMFAIL);
			transactionCoordinator.rollback(transactionXid);
		} catch (XAException tex) {
			logger.info("[{}] begin-transaction: error occurred while starting jta-transaction: {}",
					ByteUtils.byteArrayToString(compensableXid.getGlobalTransactionId()),
					ByteUtils.byteArrayToString(transactionXid.getGlobalTransactionId()), tex);
		}
	}

	// 开始 tcc 全局事务
	public void compensableBegin() throws NotSupportedException, SystemException {
		// 不支持同一个线程同时开启两个 tcc 全局事务
		if (this.getCompensableTransactionQuietly() != null) {
			throw new NotSupportedException();
		}

		// 日志管理模块 负责事务日志的存储及读取
		CompensableLogger compensableLogger = this.beanFactory.getCompensableLogger();

		// 分布式事务锁 好像0.4版本没有实现，默认返回true
		TransactionLock compensableLock = this.beanFactory.getCompensableLock();

		// TransactionRepositoryImpl 其实就是 xid - transaction 的map
		TransactionRepository compensableRepository = this.beanFactory.getCompensableRepository();

		// org.bytesoft.bytetcc.CompensableCoordinator
		// 分布式事务协调者，实现 TransactionParticipant 接口。
		// 由 RPC 自定义的拦截器(客户端)调度， 提供远程事务分支管理的接入入口
		RemoteCoordinator compensableCoordinator = this.beanFactory.getCompensableCoordinator();

		// org.bytesoft.bytejta.xa.XidFactoryImpl
		XidFactory transactionXidFactory = this.beanFactory.getTransactionXidFactory();
		// org.bytesoft.bytetcc.xa.XidFactoryImpl
		XidFactory compensableXidFactory = this.beanFactory.getCompensableXidFactory();

		// 以下两个id, 一个代表全局事务id compensableXid, 一个代表本地事务id transactionXid
		// XidFactory.TCC_FORMAT_ID 8127 + 唯一 id
		TransactionXid compensableXid = compensableXidFactory.createGlobalXid();
		// XidFactory.JTA_FORMAT_ID 1207 + 唯一 id
		TransactionXid transactionXid = transactionXidFactory.createGlobalXid(compensableXid.getGlobalTransactionId());

		// TCC 全局事务上下文
		TransactionContext compensableContext = new TransactionContext();
		compensableContext.setCoordinator(true);
		compensableContext.setCompensable(true);
		compensableContext.setXid(compensableXid);
		compensableContext.setPropagatedBy(compensableCoordinator.getIdentifier());

		// 负责全局事务相关的处理逻辑， 实现 TransactionListener 接口，
		// 在 ByteJTA 本地事务 commit/rollback 时会收到相应的通知及
		// 获得当前线程的 TCC事务， 不支持一个线程同时开启多个 TCC事务
		// 创建一个 tcc 全局事务
		CompensableTransactionImpl compensable = new CompensableTransactionImpl(compensableContext);
		compensable.setBeanFactory(this.beanFactory);

		// 将 tcc 全局事务和当前线程绑定
		this.associateThread(compensable);

		// 本地事务上下文
		TransactionContext transactionContext = new TransactionContext();
		transactionContext.setXid(transactionXid);

		boolean failure = true;
		try {
			// 开始本地事务， 同步分支事务
			this.invokeBegin(transactionContext, true);
			failure = false;
		} finally {
			if (failure) {
				this.desociateThread();
			}
		}

		// xid - 全局事务
		compensableRepository.putTransaction(compensableXid, compensable);
		// 记录事务日志
		compensableLogger.createTransaction(compensable.getTransactionArchive());
		// 0.4版本 默认返回 true
		boolean locked = compensableLock.lockTransaction(compensableXid, this.endpoint);
		if (locked == false) {
			this.invokeRollbackInBegin(transactionContext);

			compensableLogger.deleteTransaction(compensable.getTransactionArchive());
			this.desociateThread();
			compensableRepository.removeTransaction(compensableXid);

			throw new SystemException(); // should never happen
		}

		logger.info("{}| compensable transaction begin!", ByteUtils.byteArrayToString(compensableXid.getGlobalTransactionId()));
	}

	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException,
			IllegalStateException, SystemException {

		CompensableTransaction transaction = this.getCompensableTransactionQuietly();
		if (transaction == null) {
			throw new IllegalStateException();
		}
		TransactionContext transactionContext = transaction.getTransactionContext();
		boolean coordinator = transactionContext.isCoordinator();
		boolean propagated = transactionContext.isPropagated();
		boolean compensable = transactionContext.isCompensable();
		boolean compensating = transactionContext.isCompensating();
		int propagatedLevel = transactionContext.getPropagationLevel();

		if (compensable == false) {
			throw new IllegalStateException();
		} else if (compensating) {
			this.invokeTransactionCommitIfNecessary(transaction);
		} else if (coordinator) {
			if (propagated) {
				this.invokeTransactionCommitIfNecessary(transaction);
			} else if (propagatedLevel > 0) {
				this.invokeTransactionCommitIfNecessary(transaction);
			} else {
				throw new IllegalStateException();
			}
		} else {
			this.invokeTransactionCommitIfNecessary(transaction);
		}
	}

	protected void invokeTransactionCommitIfNecessary(CompensableTransaction compensable) throws RollbackException,
			HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
		// compensable.getTransaction().isMarkedRollbackOnly()
		if (compensable.getTransaction().getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK) {
			this.invokeTransactionRollback(compensable);
			throw new HeuristicRollbackException();
		} else {
			this.invokeTransactionCommit(compensable);
		}
	}

	protected void invokeTransactionCommit(CompensableTransaction compensable) throws RollbackException,
			HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {

		Transaction transaction = compensable.getTransaction();
		boolean isLocalTransaction = transaction.isLocalTransaction();
		try {
			if (isLocalTransaction) {
				this.invokeTransactionCommitIfLocalTransaction(compensable);
			} else {
				this.invokeTransactionCommitIfNotLocalTransaction(compensable);
			}
		} finally {
			compensable.setTransactionalExtra(null);
		}
	}

	protected void invokeTransactionCommitIfLocalTransaction(CompensableTransaction compensable) throws RollbackException,
			HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {

		Transaction transaction = compensable.getTransaction();
		org.bytesoft.transaction.TransactionContext transactionContext = transaction.getTransactionContext();
		RemoteCoordinator transactionCoordinator = this.beanFactory.getTransactionCoordinator();

		TransactionXid transactionXid = transactionContext.getXid();
		try {
			transactionCoordinator.end(transactionContext, XAResource.TMSUCCESS);
			transactionCoordinator.commit(transactionXid, true);
		} catch (XAException xaEx) {
			switch (xaEx.errorCode) {
			case XAException.XA_HEURCOM:
				transactionCoordinator.forgetQuietly(transactionXid);
				break;
			case XAException.XA_HEURRB:
				transactionCoordinator.forgetQuietly(transactionXid);
				HeuristicRollbackException hrex = new HeuristicRollbackException();
				hrex.initCause(xaEx);
				throw hrex;
			case XAException.XA_HEURMIX:
				transactionCoordinator.forgetQuietly(transactionXid);
				HeuristicMixedException hmex = new HeuristicMixedException();
				hmex.initCause(xaEx);
				throw hmex;
			case XAException.XAER_RMERR:
			default:
				transactionCoordinator.forgetQuietly(transactionXid); // TODO
				SystemException sysEx = new SystemException();
				sysEx.initCause(xaEx);
				throw sysEx;
			}
		}

	}

	protected void invokeTransactionCommitIfNotLocalTransaction(CompensableTransaction compensable) throws RollbackException,
			HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {

		Transaction transaction = compensable.getTransaction();
		org.bytesoft.transaction.TransactionContext transactionContext = transaction.getTransactionContext();
		RemoteCoordinator transactionCoordinator = this.beanFactory.getTransactionCoordinator();

		TransactionXid transactionXid = transactionContext.getXid();
		try {
			transactionCoordinator.end(transactionContext, XAResource.TMSUCCESS);

			TransactionContext compensableContext = compensable.getTransactionContext();
			logger.error("[{}] jta-transaction in try-phase cannot be xa transaction.",
					ByteUtils.byteArrayToString(compensableContext.getXid().getGlobalTransactionId()));

			transactionCoordinator.rollback(transactionXid);
			throw new HeuristicRollbackException();
		} catch (XAException xaEx) {
			transactionCoordinator.forgetQuietly(transactionXid);
			SystemException sysEx = new SystemException();
			sysEx.initCause(xaEx);
			throw sysEx;
		}
	}

	public void fireCompensableCommit(CompensableTransaction transaction) throws RollbackException, HeuristicMixedException,
			HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
		try {
			this.associateThread(transaction);

			transaction.commit();
		} finally {
			this.desociateThread();
		}
	}

	public void rollback() throws IllegalStateException, SecurityException, SystemException {

		CompensableTransaction transaction = this.getCompensableTransactionQuietly();
		if (transaction == null) {
			throw new IllegalStateException();
		}
		TransactionContext transactionContext = transaction.getTransactionContext();
		boolean coordinator = transactionContext.isCoordinator();
		boolean propagated = transactionContext.isPropagated();
		boolean compensable = transactionContext.isCompensable();
		boolean compensating = transactionContext.isCompensating();
		int propagatedLevel = transactionContext.getPropagationLevel();

		if (compensable == false) {
			throw new IllegalStateException();
		} else if (compensating) {
			this.invokeTransactionRollback(transaction);
		} else if (coordinator) {
			if (propagated) {
				this.invokeTransactionRollback(transaction);
			} else if (propagatedLevel > 0) {
				this.invokeTransactionRollback(transaction);
			} else {
				throw new IllegalStateException();
			}
		} else {
			this.invokeTransactionRollback(transaction);
		}

	}

	protected void invokeTransactionRollback(CompensableTransaction compensable)
			throws IllegalStateException, SecurityException, SystemException {

		Transaction transaction = compensable.getTransaction();
		org.bytesoft.transaction.TransactionContext transactionContext = transaction.getTransactionContext();
		RemoteCoordinator transactionCoordinator = this.beanFactory.getTransactionCoordinator();

		TransactionXid transactionXid = transactionContext.getXid();
		try {
			transactionCoordinator.end(transactionContext, XAResource.TMSUCCESS);
			transactionCoordinator.rollback(transactionXid);
		} catch (XAException xaEx) {
			transactionCoordinator.forgetQuietly(transactionXid);
			SystemException sysEx = new SystemException();
			sysEx.initCause(xaEx);
			throw sysEx;
		} finally {
			compensable.setTransactionalExtra(null);
		}

	}

	public void fireCompensableRollback(CompensableTransaction transaction)
			throws IllegalStateException, SecurityException, SystemException {
		try {
			this.associateThread(transaction);

			transaction.rollback();
		} finally {
			this.desociateThread();
		}
	}

	public void compensableCommit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, SystemException {
		CompensableTransaction transaction = this.getCompensableTransactionQuietly();
		if (transaction == null) {
			throw new IllegalStateException();
		} else if (transaction.getTransaction() == null) {
			throw new IllegalStateException();
		}

		TransactionContext transactionContext = transaction.getTransactionContext();
		boolean coordinator = transactionContext.isCoordinator();
		boolean compensable = transactionContext.isCompensable();
		boolean compensating = transactionContext.isCompensating();

		if (compensable == false) {
			throw new IllegalStateException();
		} else if (coordinator == false) {
			throw new IllegalStateException();
		} else if (compensating) {
			throw new IllegalStateException();
		}

		TransactionLock compensableLock = this.beanFactory.getCompensableLock();
		TransactionXid xid = transactionContext.getXid();
		boolean success = false;
		try {
			this.desociateThread();
			this.invokeCompensableCommit(transaction);
			success = true;
		} finally {
			compensableLock.unlockTransaction(xid, this.endpoint);
			if (success) {
				transaction.forgetQuietly(); // forget transaction
			} // end-if (success)
		}

	}

	protected void invokeCompensableCommit(CompensableTransaction compensable) throws RollbackException,
			HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {

		TransactionRepository compensableRepository = this.beanFactory.getCompensableRepository();
		Transaction transaction = compensable.getTransaction();
		TransactionContext compensableContext = compensable.getTransactionContext();

		boolean commitExists = false;
		boolean rollbackExists = false;
		boolean errorExists = false;

		boolean isLocalTransaction = transaction.isLocalTransaction();
		try {
			if (isLocalTransaction) /* transaction in try-phase cannot be xa transaction. */ {
				this.invokeCompensableCommitIfLocalTransaction(compensable);
				commitExists = true;
			} else {
				this.invokeCompensableCommitIfNotLocalTransaction(compensable);
			}
		} catch (HeuristicRollbackException ex) {
			logger.info("Transaction in try-phase has already been rolled back heuristically.", ex);
			rollbackExists = true;
		} catch (SystemException ex) {
			logger.info("Error occurred while committing transaction in try-phase.", ex);
			errorExists = true;
		} catch (RuntimeException ex) {
			logger.info("Error occurred while committing transaction in try-phase.", ex);
			errorExists = true;
		} finally {
			compensable.setTransactionalExtra(null);
		}

		boolean failure = true;
		try {
			if (errorExists) {
				this.fireCompensableRollback(compensable);
				failure = false;
			} else if (commitExists) {
				this.fireCompensableCommit(compensable);
				failure = false;
			} else if (rollbackExists) {
				this.fireCompensableRollback(compensable);
				failure = false;
				throw new HeuristicRollbackException();
			} else {
				failure = false;
			}
		} finally {
			TransactionXid xid = compensableContext.getXid();
			if (failure) {
				compensableRepository.putErrorTransaction(xid, compensable);
			}
		}

	}

	protected void invokeCompensableCommitIfLocalTransaction(CompensableTransaction compensable)
			throws HeuristicRollbackException, SystemException {

		RemoteCoordinator transactionCoordinator = this.beanFactory.getTransactionCoordinator();
		Transaction transaction = compensable.getTransaction();
		org.bytesoft.transaction.TransactionContext transactionContext = transaction.getTransactionContext();

		TransactionXid transactionXid = transactionContext.getXid();
		try {
			transactionCoordinator.end(transactionContext, XAResource.TMSUCCESS);
			transactionCoordinator.commit(transactionXid, true);
		} catch (XAException xaex) {
			switch (xaex.errorCode) {
			case XAException.XA_HEURCOM:
				transactionCoordinator.forgetQuietly(transactionXid);
				break;
			case XAException.XA_HEURRB:
				transactionCoordinator.forgetQuietly(transactionXid);
				HeuristicRollbackException hrex = new HeuristicRollbackException();
				hrex.initCause(xaex);
				throw hrex;
			default:
				transactionCoordinator.forgetQuietly(transactionXid); // TODO
				SystemException sysEx = new SystemException();
				sysEx.initCause(xaex);
				throw sysEx;
			}
		}
	}

	protected void invokeCompensableCommitIfNotLocalTransaction(CompensableTransaction compensable)
			throws HeuristicRollbackException, SystemException {

		RemoteCoordinator transactionCoordinator = this.beanFactory.getTransactionCoordinator();
		Transaction transaction = compensable.getTransaction();
		org.bytesoft.transaction.TransactionContext transactionContext = transaction.getTransactionContext();

		TransactionXid transactionXid = transactionContext.getXid();
		try {
			transactionCoordinator.end(transactionContext, XAResource.TMSUCCESS);
			TransactionContext compensableContext = compensable.getTransactionContext();
			logger.error("{}| jta-transaction in compensating-phase cannot be xa transaction.",
					ByteUtils.byteArrayToString(compensableContext.getXid().getGlobalTransactionId()));

			transactionCoordinator.rollback(transactionXid);
			throw new HeuristicRollbackException();
		} catch (XAException xaex) {
			transactionCoordinator.forgetQuietly(transactionXid);
			SystemException sysEx = new SystemException();
			sysEx.initCause(xaex);
			throw sysEx;
		}
	}

	public void compensableRollback() throws IllegalStateException, SecurityException, SystemException {
		CompensableTransaction transaction = this.getCompensableTransactionQuietly();
		if (transaction == null) {
			throw new IllegalStateException();
		}

		TransactionContext transactionContext = transaction.getTransactionContext();
		boolean coordinator = transactionContext.isCoordinator();
		boolean compensable = transactionContext.isCompensable();
		boolean compensating = transactionContext.isCompensating();

		if (compensable == false) {
			throw new IllegalStateException();
		} else if (coordinator == false) {
			throw new IllegalStateException();
		} else if (compensating) {
			throw new IllegalStateException();
		}

		TransactionLock compensableLock = this.beanFactory.getCompensableLock();
		TransactionXid xid = transactionContext.getXid();
		boolean success = false;
		try {
			this.desociateThread();
			this.invokeCompensableRollback(transaction);
			success = true;
		} finally {
			compensableLock.unlockTransaction(xid, this.endpoint);
			if (success) {
				transaction.forgetQuietly(); // forget transaction
			} // end-if (success)
		}

	}

	protected void invokeCompensableRollback(CompensableTransaction compensable)
			throws IllegalStateException, SecurityException, SystemException {

		TransactionRepository compensableRepository = this.beanFactory.getCompensableRepository();
		RemoteCoordinator transactionCoordinator = this.beanFactory.getTransactionCoordinator();

		Transaction transaction = compensable.getTransaction();
		org.bytesoft.compensable.TransactionContext compensableContext = compensable.getTransactionContext();
		org.bytesoft.transaction.TransactionContext transactionContext = transaction.getTransactionContext();

		TransactionXid transactionXid = transactionContext.getXid();
		try {
			transactionCoordinator.end(transactionContext, XAResource.TMSUCCESS);
			transactionCoordinator.rollback(transactionXid);
		} catch (XAException ex) {
			transactionCoordinator.forgetQuietly(transactionXid);
			logger.error("Error occurred while rolling back transaction in try phase!", ex);
		} finally {
			compensable.setTransactionalExtra(null);
		}

		boolean failure = true;
		try {
			this.fireCompensableRollback(compensable);
			failure = false;
		} finally {
			TransactionXid xid = compensableContext.getXid();
			if (failure) {
				compensableRepository.putErrorTransaction(xid, compensable);
			}
		}

	}

	public void setRollbackOnly() throws IllegalStateException, SystemException {
	}

	public void setTransactionTimeout(int seconds) throws SystemException {
	}

	public int getTimeoutSeconds() {
		return 0;
	}

	public void setTimeoutSeconds(int timeoutSeconds) {
	}

	public void setBeanFactory(CompensableBeanFactory tbf) {
		this.beanFactory = tbf;
	}

	public void setEndpoint(String identifier) {
		this.endpoint = identifier;
	}

}
