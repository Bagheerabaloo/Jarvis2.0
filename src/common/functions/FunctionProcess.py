from dataclasses import dataclass, field
import math
import psutil
import os
from threading import Thread, active_count
from threading import enumerate as thread_enumerate
from src.common.tools.library import to_int

from src.common.functions.Function import Function


@dataclass
class FunctionProcess(Function):
    name: str = 'process'

    async def state_1(self):
        def convert_size(size_bytes):
            if size_bytes == 0:
                return "0B"
            size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
            i = to_int(math.floor(math.log(size_bytes, 1024)))
            p = math.pow(1024, i)
            s = round(size_bytes / p, 2)
            return "%s %s" % (s, size_name[i])

        process = psutil.Process(os.getpid())
        text = '___ System Info ___'
        text += '\n\nRAM: ' + str(psutil.virtual_memory().percent) + '%'
        text += '\nRAM Used by Python: ' + convert_size(process.memory_info().rss)
        text += '\nRAM Used: ' + convert_size(dict(psutil.virtual_memory()._asdict())['used'])
        text += '\nRAM Total: ' + convert_size(dict(psutil.virtual_memory()._asdict())['total'])
        text += '\nCPU: ' + str(psutil.cpu_percent()) + '%'
        text += '\n\n___ System Info ___'
        text += '\n\nTotal Threads: ' + str(active_count())
        text += '\n  __Threads__'
        for thread in thread_enumerate():
            if not thread.daemon:
                text += '\n    {}'.format(thread.name)
        if any(thread for thread in thread_enumerate() if thread.daemon):
            text += '\n  __Daemon Threads__'
            for thread in [thread for thread in thread_enumerate() if thread.daemon]:
                text += '\n    {}'.format(thread.name)

        await self.send_message(chat_id=self.chat.chat_id, text=text)
        self.close_function()

