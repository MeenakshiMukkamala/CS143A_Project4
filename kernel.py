### Fill in the following information before submitting
# Group id: dugarr@uci.edu, meenakm@uci.edu, pjhunjh1@uci.edu, ado8@uci.edu
# Members: Rishita Dugar, Meenakshi Mukkamala, Pragya Jhunjhunwala, Alex Do

from collections import deque
from dataclasses import dataclass
import threading

# PID is just an integer, but it is used to make it clear when a integer is expected to be a valid PID.
PID = int

BACKGROUND: str = "Background"
FOREGROUND: str = "Foreground"

RR_QUANTUM_TICKS: int = 4
ACTIVE_QUEUE_NUM_TICKS: int = 20

MULTILEVEL: str = "Multilevel"
RR: str = "RR"
FCFS: str = "FCFS"
PRIORITY: str = "Priority"

KERNEL_RESERVED_BYTES = 10 * 1024 * 1024
HEAP_BASE = 0x20000000
STACK_TOP = 0xEFFFFFFF


class PCB:
    pid: PID
    priority: int
    num_quantum_ticks: int
    process_type: str

    def __init__(self, pid: PID, priority: int, process_type: str):
        self.pid = pid
        self.priority = priority
        self.num_quantum_ticks = 0
        self.process_type = process_type

    def __str__(self):
        return f"({self.pid}, {self.priority})"

    def __repr__(self):
        return f"({self.pid}, {self.priority})"


class Kernel:
    scheduling_algorithm: str
    ready_queue: deque[PCB]
    waiting_queue: deque[PCB]
    running: PCB
    idle_pcb: PCB
    fcfs_ready_queue: deque[PCB]
    rr_ready_queue: deque[PCB]
    active_queue: str
    active_queue_num_ticks: int

    def __init__(self, scheduling_algorithm: str, logger, mmu: "MMU", memory_size: int):
        self.scheduling_algorithm = scheduling_algorithm
        self.ready_queue = deque()
        self.waiting_queue = deque()
        self.logger = logger

        self.fcfs_ready_queue = deque()
        self.rr_ready_queue = deque()

        self.active_queue = FOREGROUND
        self.active_queue_num_ticks = 0

        self.memory_size = memory_size
        self.mmu = mmu

        self.mutexes = {}
        self.semaphores = {}
        self.lock = threading.Lock()

        self.idle_pcb = PCB(0, float("inf"), BACKGROUND)
        self.running = self.idle_pcb

        self.mmu.initialize_memory(memory_size)

    def _enqueue_ready_process(self, pcb: PCB):
        if self.scheduling_algorithm == MULTILEVEL:
            if pcb.process_type == FOREGROUND:
                self.rr_ready_queue.append(pcb)
            else:
                self.fcfs_ready_queue.append(pcb)
        else:
            self.ready_queue.append(pcb)

    def _drain_ready_queue_for_multilevel(self):
        while self.ready_queue:
            pcb = self.ready_queue.popleft()
            self._enqueue_ready_process(pcb)

    def _dispatch_active_multilevel_queue(self):
        if self.active_queue == FOREGROUND:
            self.rr_chose_next_process(self.rr_ready_queue)
        else:
            self.fcfs_chose_next_process(self.fcfs_ready_queue)

    def new_process_arrived(
        self,
        new_process: PID,
        priority: int,
        process_type: str,
        stack_memory_needed: int,
        heap_memory_needed: int
    ) -> PID:
        ok = self.mmu.allocate_process_memory(
            new_process,
            stack_memory_needed,
            heap_memory_needed
        )
        if not ok:
            return -1

        self.ready_queue.append(PCB(new_process, priority, process_type))

        if self.scheduling_algorithm == MULTILEVEL and self.running is self.idle_pcb:
            self.active_queue_num_ticks = 0

        self.choose_next_process()
        return self.running.pid

    def syscall_exit(self) -> PID:
        exiting_pid = self.running.pid
        self.running = self.idle_pcb

        if exiting_pid != 0:
            self.mmu.free_process_memory(exiting_pid)

        self.choose_next_process()
        return self.running.pid

    def syscall_set_priority(self, new_priority: int) -> PID:
        self.running.priority = new_priority
        self.choose_next_process()
        return self.running.pid

    def choose_next_process(self):
        if self.scheduling_algorithm == FCFS:
            self.fcfs_chose_next_process(self.ready_queue)

        elif self.scheduling_algorithm == PRIORITY:
            if not self.ready_queue and self.running is self.idle_pcb:
                return

            if self.running is not self.idle_pcb:
                self.ready_queue.append(self.running)

            self.running = pop_min_priority(self.ready_queue)

        elif self.scheduling_algorithm == RR:
            self.rr_chose_next_process(self.ready_queue)

        elif self.scheduling_algorithm == MULTILEVEL:
            self._drain_ready_queue_for_multilevel()
            self._dispatch_active_multilevel_queue()

            if self.running is self.idle_pcb:
                self.switch_active_queue()
                self._dispatch_active_multilevel_queue()

    def rr_chose_next_process(self, queue: deque[PCB]):
        if self.running is self.idle_pcb:
            if queue:
                self.running = queue.popleft()
        elif exceeded_quantum(self.running) and queue:
            queue.append(self.running)
            self.running = queue.popleft()

    def fcfs_chose_next_process(self, queue: deque[PCB]):
        if self.running is self.idle_pcb and queue:
            self.running = queue.popleft()

    def switch_active_queue(self):
        self.active_queue_num_ticks = 0

        if self.active_queue == FOREGROUND:
            if not self.fcfs_ready_queue:
                return

            if self.running is not self.idle_pcb:
                if exceeded_quantum(self.running):
                    self.rr_ready_queue.append(self.running)
                else:
                    self.rr_ready_queue.appendleft(self.running)
                self.running = self.idle_pcb

            self.active_queue = BACKGROUND

        else:
            if not self.rr_ready_queue:
                return

            if self.running is not self.idle_pcb:
                self.fcfs_ready_queue.appendleft(self.running)
                self.running = self.idle_pcb

            self.active_queue = FOREGROUND

    def timer_interrupt(self) -> PID:
        self.running.num_quantum_ticks += 1
        self.active_queue_num_ticks += 1

        if self.scheduling_algorithm == RR:
            self.choose_next_process()
        elif self.scheduling_algorithm == MULTILEVEL:
            if self.active_queue_num_ticks >= ACTIVE_QUEUE_NUM_TICKS:
                self.switch_active_queue()
            self.choose_next_process()

        return self.running.pid

    def syscall_init_semaphore(self, semaphore_id: int, initial_value: int):
        if initial_value < 0:
            return -1

        if semaphore_id in self.semaphores:
            return -1

        self.semaphores[semaphore_id] = {
            "value": initial_value,
            "waiting_queue": []
        }
        return 0

    def _pick_from_waiting_queue(self, waiting_queue: list[PCB]):
        if self.scheduling_algorithm == PRIORITY:
            return pop_min_priority(waiting_queue)
        return pop_min_pid(waiting_queue)

    def syscall_semaphore_p(self, semaphore_id: int) -> PID:
        semaphore = self.semaphores[semaphore_id]
        semaphore["value"] -= 1

        if semaphore["value"] < 0:
            blocked_pcb = self.running
            self.running = self.idle_pcb
            semaphore["waiting_queue"].append(blocked_pcb)
            self.choose_next_process()

        return self.running.pid

    def syscall_semaphore_v(self, semaphore_id: int) -> PID:
        semaphore = self.semaphores[semaphore_id]
        semaphore["value"] += 1

        if semaphore["value"] <= 0 and semaphore["waiting_queue"]:
            released_pcb = self._pick_from_waiting_queue(semaphore["waiting_queue"])
            self.ready_queue.append(released_pcb)

        return self.running.pid

    def syscall_init_mutex(self, mutex_id: int):
        with self.lock:
            if mutex_id in self.mutexes:
                return -1

            self.mutexes[mutex_id] = {
                "held_by": None,
                "waiting_queue": []
            }
            return 0

    def syscall_mutex_lock(self, mutex_id: int) -> PID:
        mutex = self.mutexes[mutex_id]

        if mutex["held_by"] is None:
            mutex["held_by"] = self.running.pid
        else:
            blocked_pcb = self.running
            self.running = self.idle_pcb
            mutex["waiting_queue"].append(blocked_pcb)
            self.choose_next_process()

        return self.running.pid

    def syscall_mutex_unlock(self, mutex_id: int) -> PID:
        mutex = self.mutexes[mutex_id]

        if mutex["waiting_queue"]:
            released_pcb = self._pick_from_waiting_queue(mutex["waiting_queue"])
            mutex["held_by"] = released_pcb.pid
            self.ready_queue.append(released_pcb)
        else:
            mutex["held_by"] = None

        return self.running.pid


def exceeded_quantum(pcb: PCB) -> bool:
    if pcb.num_quantum_ticks >= RR_QUANTUM_TICKS:
        pcb.num_quantum_ticks = 0
        return True
    return False

def pop_min_priority(pcbs: list[PCB]) -> PCB:
    min_index = 0
    for i in range(1, len(pcbs)):
        process = pcbs[i]
        if process.priority < pcbs[min_index].priority:
            min_index = i
        elif process.priority == pcbs[min_index].priority and process.pid < pcbs[min_index].pid:
            min_index = i

    popped = pcbs[min_index]
    del pcbs[min_index]
    return popped


def pop_min_pid(pcbs: list[PCB]) -> PCB:
    lowest_pid_i = 0
    for i in range(1, len(pcbs)):
        if pcbs[i].pid < pcbs[lowest_pid_i].pid:
            lowest_pid_i = i

    popped = pcbs[lowest_pid_i]
    del pcbs[lowest_pid_i]
    return popped


class MMU:
    def __init__(self, logger):
        self.logger = logger
        self.memory_size = 0
        self.free_holes = []
        self.process_table = {}

    def initialize_memory(self, memory_size: int):
        self.memory_size = memory_size
        self.free_holes = []
        self.process_table = {}

        usable_start = KERNEL_RESERVED_BYTES
        if memory_size > usable_start:
            self.free_holes.append((usable_start, memory_size - usable_start))

    def _allocate_best_fit(self, size: int):
        if size == 0:
            return None

        best_index = None
        best_hole = None

        for i, (start, hole_size) in enumerate(self.free_holes):
            if hole_size >= size:
                if best_hole is None:
                    best_index = i
                    best_hole = (start, hole_size)
                else:
                    best_start, best_size = best_hole
                    if hole_size < best_size or (hole_size == best_size and start < best_start):
                        best_index = i
                        best_hole = (start, hole_size)

        if best_hole is None:
            return None

        start, hole_size = best_hole
        allocated = (start, size)

        if hole_size == size:
            del self.free_holes[best_index]
        else:
            self.free_holes[best_index] = (start + size, hole_size - size)

        return allocated

    def _insert_and_coalesce(self, start: int, size: int):
        if size == 0:
            return

        self.free_holes.append((start, size))
        self.free_holes.sort()

        merged = []
        for cur_start, cur_size in self.free_holes:
            if not merged:
                merged.append((cur_start, cur_size))
                continue

            prev_start, prev_size = merged[-1]
            prev_end = prev_start + prev_size

            if prev_end == cur_start:
                merged[-1] = (prev_start, prev_size + cur_size)
            else:
                merged.append((cur_start, cur_size))

        self.free_holes = merged

    def allocate_process_memory(self, pid: PID, stack_memory_needed: int, heap_memory_needed: int) -> bool:
        stack_segment = self._allocate_best_fit(stack_memory_needed)
        if stack_memory_needed > 0 and stack_segment is None:
            return False

        heap_segment = self._allocate_best_fit(heap_memory_needed)
        if heap_memory_needed > 0 and heap_segment is None:
            if stack_segment is not None:
                self._insert_and_coalesce(stack_segment[0], stack_segment[1])
            return False

        self.process_table[pid] = {
            "stack_size": stack_memory_needed,
            "heap_size": heap_memory_needed,
            "stack_segment": stack_segment,
            "heap_segment": heap_segment,
        }
        return True

    def free_process_memory(self, pid: PID):
        if pid not in self.process_table:
            return

        info = self.process_table.pop(pid)

        stack_segment = info["stack_segment"]
        heap_segment = info["heap_segment"]

        if stack_segment is not None:
            self._insert_and_coalesce(stack_segment[0], stack_segment[1])

        if heap_segment is not None:
            self._insert_and_coalesce(heap_segment[0], heap_segment[1])

    def translate(self, address: int, pid: PID) -> int | None:
        if pid not in self.process_table:
            return None

        info = self.process_table[pid]

        heap_size = info["heap_size"]
        stack_size = info["stack_size"]
        heap_segment = info["heap_segment"]
        stack_segment = info["stack_segment"]

        if heap_size > 0:
            heap_start_vaddr = HEAP_BASE
            heap_end_vaddr = HEAP_BASE + heap_size - 1

            if heap_start_vaddr <= address <= heap_end_vaddr:
                offset = address - HEAP_BASE
                return heap_segment[0] + offset

        if stack_size > 0:
            stack_low_vaddr = STACK_TOP - stack_size + 1
            stack_high_vaddr = STACK_TOP

            if stack_low_vaddr <= address <= stack_high_vaddr:
                offset = address - stack_low_vaddr
                return stack_segment[0] + offset

        return None