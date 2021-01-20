# TiCDC Open Protocol 测试集

## 目录结构
```
.
├── README.md 自述文件
├── expand_char 扩展字符用例
│   ├── expected.txt
│   ├── key.bin
│   └── value.bin
├── lang_event 大尺寸事件用例
│   ├── expected.txt
│   ├── key.bin
│   └── value.bin
└── simple 基本用例
    ├── expected.txt
    ├── key.bin
    └── value.bin

```

## key.bin 文件说明

`key.bin` 文件是 TiCDC Open Protocol 输出的 Key 的示例。

## value.bin 文件说明

`value.bin` 文件是 TiCDC Open Protocol 输出的 Value 的示例。

## expected.txt 文件说明

`expected.txt` 文件是从 `key.bin` 和 `value.bin` 解析出来的 JSON 串，用于对比和验证程序正确性。
`expected.txt` 文件格式遵循如下规则：
```
Event1 Key Json
Event1 Value Json

Event2 Key Json
Event2 Value Json

Event3 Key Json
Event3 Value Json

Event4 Key Json
Event4 Value Json

...
```
