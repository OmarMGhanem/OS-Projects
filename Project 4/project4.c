// Kernel module
// Authors: Doğa Dikbayır, Süleyman Yasir Kula, Tan Küçükoğlu

#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/kernel.h>
#include <linux/init.h>
#include <linux/errno.h>
#include <linux/sched.h>
#include <linux/stat.h>

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Doga Dikbayir, Suleyman Yasir Kula, Tan Kucukoglu");
MODULE_DESCRIPTION("Project 4");

static int inpid = 1234;

module_param(inpid, int, 0);
MODULE_PARM_DESC(inpid, "process id");
    
int init_module(void){
	struct task_struct *task;
    struct task_struct ts;

    printk("***Start of module***\n");

    for_each_process(task){

    	struct list_head *child;

		if(task->pid == (pid_t)inpid){

	   		printk("Process name: %s [%d]\n",task->comm ,task->pid);
			printk("Start address: %lu, End address: %lu\n", task->mm->start_code, task->mm->end_code);
			printk("Total vm: %lu\n", task->mm->total_vm);

	   		printk("Parent process: %s [%d]\n", (task->parent)->comm, (task->parent)->pid);
			printk("Start address: %lu, End address: %lu\n", (task->parent)->mm->start_code, (task->parent)->mm->end_code);
			printk("Total vm: %lu\n", (task->parent)->mm->total_vm);

	   		printk("Children:\n");
	   		list_for_each(child, &(task->children)){

				ts = *list_entry(child, struct task_struct, sibling);
				printk("Child: %s [%d]\n", (ts.comm), ts.pid);
				printk("Start address: %lu, End address: %lu\n", ts.mm->start_code, ts.mm->end_code);
				printk("Total vm: %lu\n", ts.mm->total_vm);
    		}
		}
	}
	printk("***End of module***\n");
    return 0;
}
void cleanup_module(void){
    printk(KERN_INFO "***Removing module***\n");
}