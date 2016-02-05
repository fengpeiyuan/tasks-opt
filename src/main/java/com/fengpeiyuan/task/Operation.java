package com.fengpeiyuan.task;

import com.fengpeiyuan.dao.redis.shard.RedisShard;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Operation {
	private RedisShard redisShard;
	final static Logger logger = Logger.getLogger(Operation.class);

	private  String sharding = "1";
	private  String undoTaskQueue = "_undoTaskQueue";
	private  String doneTaskQueue = "_doneTaskQueue";


	/**
	 *
	 * @param mainId
	 * @param subtaskIdFrom
	 * @param subtaskIdTo
     * @return false: fail
	 *         true : success
     */

	/*eval "local tonumber=tonumber local undokeyqueue=KEYS[1] local maintaskid=KEYS[2] local subtaskid='_sub_'..KEYS[2]  local i for i=tonumber(KEYS[3]),tonumber(KEYS[4]) do redis.call('SETBIT',subtaskid,i,'1') end  redis.call('LPUSH',undokeyqueue,maintaskid) return true " 4 undoqueue 10001 10 1000 */
	private static String scriptFillTaskInLua="local tonumber=tonumber local undokeyqueue=KEYS[1] local maintaskid=KEYS[2] local subtaskid='_sub_'..KEYS[2]  local i for i=tonumber(KEYS[3]),tonumber(KEYS[4]) do redis.call('SETBIT',subtaskid,i,'1') end  redis.call('LPUSH',undokeyqueue,maintaskid) return true ";
	private static String shaFillTaskInLua;
	public Boolean fillMaintaskAndSubtasks(String mainId,Integer subtaskIdFrom, Integer subtaskIdTo) {
		if(null==mainId || "".equals(mainId) || subtaskIdFrom<0 ||subtaskIdTo<0){
			logger.error("Error! invalid input parameters,mainId:"+mainId+",subtaskIdFrom:"+subtaskIdFrom+",subtaskIdTo:"+subtaskIdTo);
			return Boolean.FALSE;
		}
		Object result;
		try {
			if(null == Operation.shaFillTaskInLua){
				if(this.getRedisShard().scriptExistsSingleShard(getSharding(),"603b9a69d2c36aff45803c35867ae3c87e653d09")){
					Operation.shaFillTaskInLua = "603b9a69d2c36aff45803c35867ae3c87e653d09";
					result = this.getRedisShard().evalshaSingleShard(getSharding(),Operation.shaFillTaskInLua,4,getUndoTaskQueue(),mainId,subtaskIdFrom.toString(),subtaskIdTo.toString());
				}else {
					Operation.shaFillTaskInLua = this.getRedisShard().scriptLoadSingleShard(getSharding(), Operation.scriptFillTaskInLua);
					System.out.print(Operation.shaFillTaskInLua);
					result = this.getRedisShard().evalshaSingleShard(getSharding(), Operation.shaFillTaskInLua, 4,getUndoTaskQueue(),mainId,subtaskIdFrom.toString(),subtaskIdTo.toString());
				}
			}else{
				result = this.getRedisShard().evalshaSingleShard(getSharding(),Operation.shaFillTaskInLua,4,getUndoTaskQueue(),mainId,subtaskIdFrom.toString(),subtaskIdTo.toString());
			}

		}catch (Exception t){
			logger.error("error happend when fill tasks in lua",t);
			return Boolean.FALSE;
		}
		return Boolean.TRUE;
	}




	
	public RedisShard getRedisShard() {
		return redisShard;
	}

	public void setRedisShard(RedisShard redisShard) {
		this.redisShard = redisShard;
	}

	public String getSharding() {
		return sharding;
	}

	public void setSharding(String sharding) {
		this.sharding = sharding;
	}

	public String getUndoTaskQueue() {
		return undoTaskQueue;
	}

	public void setUndoTaskQueue(String undoTaskQueue) {
		this.undoTaskQueue = undoTaskQueue;
	}

	public String getDoneTaskQueue() {
		return doneTaskQueue;
	}

	public void setDoneTaskQueue(String doneTaskQueue) {
		this.doneTaskQueue = doneTaskQueue;
	}
}
