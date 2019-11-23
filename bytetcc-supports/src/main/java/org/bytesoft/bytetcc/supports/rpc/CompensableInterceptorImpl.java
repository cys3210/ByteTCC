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
package org.bytesoft.bytetcc.supports.rpc;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.bytejta.supports.rpc.TransactionRequestImpl;
import org.bytesoft.bytejta.supports.rpc.TransactionResponseImpl;
import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.bytetcc.CompensableCoordinator;
import org.bytesoft.compensable.CompensableBeanFactory;
import org.bytesoft.compensable.CompensableManager;
import org.bytesoft.compensable.CompensableTransaction;
import org.bytesoft.compensable.aware.CompensableBeanFactoryAware;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.supports.rpc.TransactionInterceptor;
import org.bytesoft.transaction.supports.rpc.TransactionRequest;
import org.bytesoft.transaction.supports.rpc.TransactionResponse;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompensableInterceptorImpl implements TransactionInterceptor, CompensableBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(CompensableInterceptorImpl.class);

	@javax.inject.Inject
	private CompensableBeanFactory beanFactory;

	// 在发送全局事务请求前 在本地创建一个代表其的分支事务，进行管理
	public void beforeSendRequest(TransactionRequest request) throws IllegalStateException {
		// tcc 全局事务管理器
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();
		XidFactory xidFactory = this.beanFactory.getCompensableXidFactory();
		// tcc 全局事务
		CompensableTransaction transaction = compensableManager.getCompensableTransactionQuietly();
		if (transaction == null) {
			return;
		}

		// tcc 全局事务上下文
		TransactionContext srcTransactionContext = transaction.getTransactionContext();
		TransactionContext transactionContext = srcTransactionContext.clone();
		TransactionXid currentXid = srcTransactionContext.getXid();
		TransactionXid globalXid = xidFactory.createGlobalXid(currentXid.getGlobalTransactionId());
		transactionContext.setXid(globalXid);
		// tcc 全局事务上下文与 request 进行关联
		request.setTransactionContext(transactionContext);

		// 检测事务状态， 状态为 rollback 则不进行事务传播
		if (transaction.getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK) {
			throw new IllegalStateException(
					"Transaction has been marked as rollback only, can not propagate its context to remote branch.");
		} // end-if (transaction.getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK)


		try {
			// 获得 tcc 分支事务协助者, spring cloud 使用 SpringCloudCoordinator
			RemoteCoordinator resource = request.getTargetTransactionCoordinator();
			RemoteResourceDescriptor descriptor = new RemoteResourceDescriptor();
			descriptor.setDelegate(resource);
			descriptor.setIdentifier(resource.getIdentifier());

			// tcc 分支事务协助组件相关资源加入到全局事务中
			boolean participantEnlisted = transaction.enlistResource(descriptor);
			((TransactionRequestImpl) request).setParticipantEnlistFlag(participantEnlisted);
		} catch (IllegalStateException ex) {
			logger.error("CompensableInterceptorImpl.beforeSendRequest({})", request, ex);
			throw ex;
		} catch (RollbackException ex) {
			transaction.setRollbackOnlyQuietly();
			logger.error("CompensableInterceptorImpl.beforeSendRequest({})", request, ex);
			throw new IllegalStateException(ex);
		} catch (SystemException ex) {
			logger.error("CompensableInterceptorImpl.beforeSendRequest({})", request, ex);
			throw new IllegalStateException(ex);
		}
	}

	// 接收到全局事务请求后就开始一个 tcc分支事务 (被调用方)
	public void afterReceiveRequest(TransactionRequest request) throws IllegalStateException {
		TransactionContext srcTransactionContext = request.getTransactionContext();
		if (srcTransactionContext == null) {
			return;
		}

		RemoteCoordinator compensableCoordinator = this.beanFactory.getCompensableCoordinator();
		TransactionContext transactionContext = srcTransactionContext.clone();
		transactionContext.setPropagatedBy(srcTransactionContext.getPropagatedBy());
		try {

			compensableCoordinator.start(transactionContext, XAResource.TMNOFLAGS);
		} catch (XAException ex) {
			logger.error("CompensableInterceptorImpl.afterReceiveRequest({})", request, ex);
			IllegalStateException exception = new IllegalStateException();
			exception.initCause(ex);
			throw exception;
		}

	}

	// 在发送全局事务响应消息之前， 结束掉tcc事务 (被调用方)
	public void beforeSendResponse(TransactionResponse response) throws IllegalStateException {
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();
		CompensableTransaction transaction = compensableManager.getCompensableTransactionQuietly();
		if (transaction == null) {
			return;
		}

		// CompensableCoordinator
		RemoteCoordinator compensableCoordinator = this.beanFactory.getCompensableCoordinator();

		// 获得全局事务上下文，设置到 response 中， 返回给主调用者
		TransactionContext srcTransactionContext = transaction.getTransactionContext();
		TransactionContext transactionContext = srcTransactionContext.clone();
		transactionContext.setPropagatedBy(srcTransactionContext.getPropagatedBy());
		response.setTransactionContext(transactionContext);
		try {
			compensableCoordinator.end(transactionContext, XAResource.TMSUCCESS);
		} catch (XAException ex) {
			logger.error("CompensableInterceptorImpl.beforeSendResponse({})", response, ex);
			IllegalStateException exception = new IllegalStateException();
			exception.initCause(ex);
			throw exception;
		}
	}

	// 收到全局事务响应后
	public void afterReceiveResponse(TransactionResponse response) throws IllegalStateException {
		CompensableManager compensableManager = this.beanFactory.getCompensableManager();
		TransactionContext remoteTransactionContext = response.getTransactionContext();
		CompensableTransaction transaction = compensableManager.getCompensableTransactionQuietly();

		boolean participantEnlistFlag = ((TransactionResponseImpl) response).isParticipantEnlistFlag();
		boolean participantDelistFlag = ((TransactionResponseImpl) response).isParticipantDelistFlag();

		RemoteCoordinator resource = response.getSourceTransactionCoordinator();

		if (transaction == null || remoteTransactionContext == null) {
			return;
		} else if (participantEnlistFlag == false) {
			return;
		} else if (resource == null) {
			logger.error("CompensableInterceptorImpl.afterReceiveResponse(TransactionRequest): remote coordinator is null.");
			throw new IllegalStateException("remote coordinator is null.");
		}

		try {

			RemoteResourceDescriptor descriptor = new RemoteResourceDescriptor();
			descriptor.setDelegate(resource);
			// descriptor.setIdentifier(resource.getIdentifier());

			transaction.delistResource(descriptor, participantDelistFlag ? XAResource.TMFAIL : XAResource.TMSUCCESS);
		} catch (IllegalStateException ex) {
			logger.error("CompensableInterceptorImpl.afterReceiveResponse({})", response, ex);
			throw ex;
		} catch (SystemException ex) {
			logger.error("CompensableInterceptorImpl.afterReceiveResponse({})", response, ex);
			throw new IllegalStateException(ex);
		}
	}

	public void setBeanFactory(CompensableBeanFactory tbf) {
		this.beanFactory = tbf;
	}

	public CompensableBeanFactory getBeanFactory() {
		return beanFactory;
	}

}
