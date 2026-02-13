# Stream Processing you can actually debug.
A cloud-native, developer-first streaming engine built on Rust, Timely Dataflow, and SlateDB.

## 🧐 Why Rill?
We built Rill because we were tired of the "Streaming Tax": the operational complexity, the inability to debug production crashes, and the massive infrastructure required just to run "Hello World."

#### Rill is different:
🐛 Debuggable by Default: Replay production bugs locally with Time Travel Debugging. Step through your streaming operators in VSCode/LLDB just like a standard microservice.

🚀 Serverless Scaling: Powered by SlateDB (LSM on S3) and Timely Dataflow. Workers are stateless; adding a node instantly redistributes load without downloading terabytes of RocksDB checkpoints.

💻 Local-First: No MiniCluster. No JVM. No ZooKeeper. cargo run starts the full engine on your laptop using local disk. Deploying to prod is just changing a config flag.

⚡ Unmatched Performance: Zero-cost abstractions, zero-copy networking (Arrow Flight), and true cyclic graph support for complex event processing (CEP).

## 🏗️ Architecture
Rill decouples compute from storage to enable instant scaling.

```mermaid
graph TD
    subgraph "Data Plane"
        Source[Source] --> Operator[Async Operator]
        Operator --> Sink[Sink]
    end

    subgraph "Storage Hierarchy (The Context)"
        Operator <--> L1[L1: MemTable (RAM)]
        L1 <--> L2[L2: Foyer (Local NVMe)]
        L2 <--> L3[L3: SlateDB (S3 Object Store)]
    end
```
### Key Components:
Control Plane: Lightweight Raft-based coordination (JobManager).
Data Plane: Timely Dataflow workers wrapping our custom AsyncOperator.
Storage: A three-tier cache (RAM -> NVMe -> S3) that hides cloud latency.

## 📄 License
This project is licensed under the Apache 2.0 License.