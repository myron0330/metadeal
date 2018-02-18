# encoding: UTF-8

# 系统模块
from Queue import Queue, Empty
from threading import Thread
from time import sleep
from collections import defaultdict

# 第三方模块
from PyQt4.QtCore import QTimer

# 自己开发的模块
from trader.ctp_trader.event_type import *

class Event:
    """事件对象"""

    #----------------------------------------------------------------------
    def __init__(self, type_=None):
        """Constructor"""
        self.type_ = type_      # 事件类型
        self.dict_ = {}         # 字典用于保存具体的事件数据


class EventEngine(object):
    """
    计时器使用python线程的事件驱动引擎        
    """
    #----------------------------------------------------------------------
    def __init__(self):
        """初始化事件引擎"""
        # 事件队列
        self._queue = Queue()
        
        # 事件引擎开关
        self._active = False
        
        # 事件处理线程
        self._thread = Thread(target = self._run)
        
        # 计时器，用于触发计时器事件
        self._timer = Thread(target = self._run_timer)
        self._timerActive = False                      # 计时器工作状态
        self._timerSleep = 1                           # 计时器触发间隔（默认1秒）
        
        # 这里的__handlers是一个字典，用来保存对应的事件调用关系
        # 其中每个键对应的值是一个列表，列表中保存了对该事件进行监听的函数功能
        self._handlers = defaultdict(list)
        
    #----------------------------------------------------------------------
    def _run(self):
        """引擎运行"""
        while self._active == True:
            try:
                event = self._queue.get(block = True, timeout = 1)  # 获取事件的阻塞时间设为1秒
                self._process(event)
            except Empty:
                pass
            
    #----------------------------------------------------------------------
    def _process(self, event):
        """处理事件"""
        # 检查是否存在对该事件进行监听的处理函数
        if event.type_ in self._handlers:
            # 若存在，则按顺序将事件传递给处理函数执行
            [handler(event) for handler in self._handlers[event.type_]]
            
            # 以上语句为Python列表解析方式的写法，对应的常规循环写法为：
            #for handler in self.__handlers[event.type_]:
                #handler(event)    
               
    #----------------------------------------------------------------------
    def _run_timer(self):
        """运行在计时器线程中的循环函数"""
        while self._timerActive:
            # 创建计时器事件
            event = Event(type_=EVENT_TIMER)
        
            # 向队列中存入计时器事件
            self.put(event)    
            
            # 等待
            sleep(self._timerSleep)

    #----------------------------------------------------------------------
    def start(self):
        """引擎启动"""
        # 将引擎设为启动
        self._active = True
        
        # 启动事件处理线程
        self._thread.start()
        
        # 启动计时器，计时器事件间隔默认设定为1秒
        self._timerActive = True
        self._timer.start()
    
    #----------------------------------------------------------------------
    def stop(self):
        """停止引擎"""
        # 将引擎设为停止
        self._active = False
        
        # 停止计时器
        self._timerActive = False
        self._timer.join()
        
        # 等待事件处理线程退出
        self._thread.join()
            
    #----------------------------------------------------------------------
    def register(self, type_, handler):
        """注册事件处理函数监听"""
        # 尝试获取该事件类型对应的处理函数列表，若无defaultDict会自动创建新的list
        handlerList = self._handlers[type_]
        
        # 若要注册的处理器不在该事件的处理器列表中，则注册该事件
        if handler not in handlerList:
            handlerList.append(handler)
            
    #----------------------------------------------------------------------
    def deregister(self, type_, handler):
        """注销事件处理函数监听"""
        # 尝试获取该事件类型对应的处理函数列表，若无则忽略该次注销请求   
        handlerList = self._handlers[type_]
            
        # 如果该函数存在于列表中，则移除
        if handler in handlerList:
            handlerList.remove(handler)

        # 如果函数列表为空，则从引擎中移除该事件类型
        if not handlerList:
            del self._handlers[type_]
        
    #----------------------------------------------------------------------
    def put(self, event):
        """向事件队列中存入事件"""
        self._queue.put(event)

#----------------------------------------------------------------------
def test():
    """测试函数"""
    import sys
    from datetime import datetime
    from PyQt4.QtCore import QCoreApplication
    
    def simpletest(event):
        print u'处理每秒触发的计时器事件：%s' % str(datetime.now())
    
    app = QCoreApplication(sys.argv)
    
    ee = EventEngine()
    ee.register(EVENT_TIMER, simpletest)
    ee.start()
    
    app.exec_()
    
    
# 直接运行脚本可以进行测试
if __name__ == '__main__':
    test()