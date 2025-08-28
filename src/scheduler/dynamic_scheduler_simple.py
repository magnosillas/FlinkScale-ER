"""
Simplified Dynamic Scheduler - Works without Kafka
"""
import docker
import time
import random
import requests

class MetricsMonitor:
    def __init__(self):
        self.high_threshold = 0.7
        self.low_threshold = 0.3
        
    def get_backpressure(self):
        """Simulate backpressure for testing"""
        # Simulate a pattern: gradually increase then decrease
        t = time.time()
        cycle = t % 300  # 5-minute cycles
        if cycle < 150:
            return cycle / 150  # Increase from 0 to 1
        else:
            return 2 - (cycle / 150)  # Decrease from 1 to 0
    
    def should_scale(self):
        bp = self.get_backpressure()
        if bp > self.high_threshold:
            return 'SCALE_UP', bp
        elif bp < self.low_threshold:
            return 'SCALE_DOWN', bp
        return 'MAINTAIN', bp

class DynamicScheduler:
    def __init__(self, min_nodes=1, max_nodes=4):
        print("🚀 Initializing Simplified Dynamic Scheduler...")
        self.docker_client = docker.from_env()
        self.monitor = MetricsMonitor()
        self.min_nodes = min_nodes
        self.max_nodes = max_nodes
        self.current_nodes = self.count_taskmanagers()
        self.last_scale_time = 0
        self.cooldown = 30
        
    def count_taskmanagers(self):
        count = 0
        try:
            for container in self.docker_client.containers.list():
                if 'taskmanager' in container.name:
                    count += 1
        except:
            count = 1
        return max(1, count)
    
    def can_scale(self):
        return (time.time() - self.last_scale_time) > self.cooldown
        
    def scale_up(self):
        if self.current_nodes >= self.max_nodes:
            print(f"  ⚠️  At maximum ({self.max_nodes} nodes)")
            return
            
        if not self.can_scale():
            remaining = self.cooldown - (time.time() - self.last_scale_time)
            print(f"  ⏳ Cooldown: {remaining:.0f}s remaining")
            return
            
        self.current_nodes += 1
        print(f"  📈 SCALING UP to {self.current_nodes} nodes")
        
        try:
            # Get the network name dynamically
            networks = self.docker_client.networks.list()
            network_name = None
            for net in networks:
                if 'flink-network' in net.name:
                    network_name = net.name
                    break
            
            if not network_name:
                # Try default network
                network_name = "dynamic-entity-blocking_default"
            
            container = self.docker_client.containers.run(
                "flink:1.14-scala_2.12",
                command="taskmanager",
                environment={"JOB_MANAGER_RPC_ADDRESS": "jobmanager"},
                network=network_name,
                detach=True,
                name=f"taskmanager-{self.current_nodes}"
            )
            print(f"  ✅ Created: {container.name}")
            self.last_scale_time = time.time()
        except Exception as e:
            print(f"  ❌ Error: {e}")
            self.current_nodes -= 1
    
    def scale_down(self):
        if self.current_nodes <= self.min_nodes:
            print(f"  ⚠️  At minimum ({self.min_nodes} nodes)")
            return
            
        if not self.can_scale():
            remaining = self.cooldown - (time.time() - self.last_scale_time)
            print(f"  ⏳ Cooldown: {remaining:.0f}s remaining")
            return
            
        print(f"  📉 SCALING DOWN to {self.current_nodes-1} nodes")
        
        try:
            container = self.docker_client.containers.get(
                f"taskmanager-{self.current_nodes}"
            )
            container.stop(timeout=10)
            container.remove()
            print(f"  ✅ Removed: taskmanager-{self.current_nodes}")
            self.current_nodes -= 1
            self.last_scale_time = time.time()
        except Exception as e:
            print(f"  ❌ Error: {e}")
    
    def run(self):
        print("\n" + "="*60)
        print("   DYNAMIC RESOURCE SCHEDULER - SIMPLIFIED VERSION")
        print("="*60)
        print(f"📊 Configuration:")
        print(f"   Min nodes: {self.min_nodes}")
        print(f"   Max nodes: {self.max_nodes}")
        print(f"   Current: {self.current_nodes}")
        print(f"   Cooldown: {self.cooldown}s")
        print("="*60 + "\n")
        
        iteration = 0
        try:
            while True:
                iteration += 1
                action, bp = self.monitor.should_scale()
                
                print(f"\n[{time.strftime('%H:%M:%S')}] Check #{iteration}")
                print(f"  📊 Backpressure: {bp:.1%}")
                print(f"  🖥️  Nodes: {self.current_nodes}")
                print(f"  🎯 Decision: {action}")
                
                if action == 'SCALE_UP':
                    self.scale_up()
                elif action == 'SCALE_DOWN':
                    self.scale_down()
                else:
                    print(f"  ✅ Stable")
                
                time.sleep(10)  # Check every 10 seconds for demo
                
        except KeyboardInterrupt:
            print("\n\n⏹️  Scheduler stopped by user")
            print(f"Final state: {self.current_nodes} nodes")

if __name__ == "__main__":
    scheduler = DynamicScheduler(min_nodes=1, max_nodes=4)
    scheduler.run()
