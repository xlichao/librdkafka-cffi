from typing import TextIO,Callable


class Processor:
    def __init__(self,source_file:TextIO,target_file:TextIO,select_filename:str) -> None:
        self.source_file = source_file
        self.target_file = target_file
        self.select_filename = select_filename
        # 处理状态
        self.line = None
        self.line_index = 0
        self.current_file = ""
        self.has_error = False
        # 处理流程
        self.pipeline:list[Callable] = [
            self._locate_current_filename,
            self._remove_attribute,
            self._output,
        ]
    
    def process(self):
        self._add_addtional()
        for line_index,line in enumerate(self.source_file):
            self.line = line.strip("\n")
            self.line_index = line_index
            self._pipeline()
        if self.has_error:
            print("预处理存在错误，需要检查以上输出具体行数")
    
    def _pipeline(self):
        for method in self.pipeline:
            method()
        
    def _locate_current_filename(self):
        if self.line.startswith("#"):
            first_quote = self.line.find('"')
            last_quote = self.line.find('"',first_quote + 1)
            if first_quote == -1 or last_quote == -1:
                print(f'[{self.line_index}] 存在未匹配的引号 {self.line}')
                self.has_error = True
                return
            self.current_file = self.line[first_quote + 1:last_quote]
    
    def _remove_attribute(self):
        attribute_str = "__attribute__"
        start_index = self.line.find(attribute_str)
        if start_index == -1:
            return
        left_quote_index = self.line.find("(",start_index)
        if left_quote_index == -1:
            print(f"[{self.line_index}] 发现 __attribute__ 但是未发现括号 {self.line} (可能跨行？)")
            return
        pair_count = 0
        for end_index,char in enumerate(self.line[left_quote_index:]):
            if char == "(":
                pair_count += 1
            if char == ")":
                pair_count -= 1
            if pair_count == 0:
                break
        end_index += left_quote_index
        new_line = self.line[:start_index] + self.line[end_index + 1:]
        print(f"[{self.line_index}] 发现 __attribute__ : {self.line} -> {new_line}")
        self.line = new_line


    def _output(self):
        if self.line.startswith("#"):
            return
        if self.current_file == self.select_filename:
            if self.line.endswith("\n"):
                print(self.line,file=self.target_file,end="")
            else:
                print(self.line,file=self.target_file)
    

    def _add_addtional(self):
        infos = [
            'typedef int... mode_t;',
            'typedef int... off_t;',
            # 'extern int32_t RD_KAFKA_PARTITION_UA=((int32_t)-1);',
        ]
        for info in infos:
            print(info,file=self.target_file)
        




