import { IExecutor } from './Executor';
import ITask from './Task';

function getNextItem<T>(queue: AsyncIterable<T>): Promise<IteratorResult<T>> {
    const iterator = queue[Symbol.asyncIterator](); 
    return iterator.next(); 
}

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0): Promise<void> {
    const runningTasks = new Set<number>();
    const activePromises: Promise<void>[] = [];
    const await_tasks = new Array();        

    const executeTask = async (task: ITask): Promise<void> => {
        try {
            runningTasks.add(task.targetId);
            await executor.executeTask(task);
        } finally {
            runningTasks.delete(task.targetId);
        }
    };

    const getNextTaskFromIterate = async function(){

        let tmp = await getNextItem(queue);
        let tmp_task = tmp.value;

        while (tmp.done==false && runningTasks.has(tmp_task.targetId)) {
            await_tasks.push(tmp);
            tmp = await getNextItem(queue);
            tmp_task = tmp.value;
        }

        return tmp; 
    }

    const getNextTaskFromArray = async function(){
        for (let i = 0; i < await_tasks.length; i++){
            if(!runningTasks.has(await_tasks[i].value.targetId)){
                const ret = await_tasks[i];
                await_tasks.splice(i, 1);
                return ret;
            }
        }
        return null;
    }

    var cur_task = await getNextTaskFromIterate();
    while(cur_task.done == false){
        var task = cur_task.value;    
        while (maxThreads > 0 && activePromises.length >= maxThreads) {
            await Promise.race(activePromises);
        }

        const taskPromise = executeTask(task).finally(() => {
            const index = activePromises.indexOf(taskPromise);
            if (index > -1) {
                activePromises.splice(index, 1);
            }

        });

        activePromises.push(taskPromise);

        var next = await getNextTaskFromArray();
        if(next == null) 
            next = await getNextTaskFromIterate();
        if(next.done == true){
            await Promise.race(activePromises);
            next = await getNextTaskFromArray();
            if(next == null ){
                if(await_tasks.length > 0){
                    while(next == null){
                        await Promise.race(activePromises);
                        next = await getNextTaskFromArray();
                    }
                }else{
                    await sleep(20);//when all tasks are done, wait for new task in 20 ms.
                    next = await getNextTaskFromIterate();
                } 
            }  
        }
        cur_task = next;
    }
    await Promise.all(activePromises);
}

async function sleep(ms: number) {
    ms = Math.max(0, ms);
    // ms += (Math.random() - 0.5) * ms / 10;
    return new Promise<void>(r => setTimeout(() => r(), ms));
}
