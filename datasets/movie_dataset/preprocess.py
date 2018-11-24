FROM = []
TO = []

with open('movie_conversations.tsv', buffering=1000) as f:
    for row in f:
        data = row.split('	')[3][1:-2].split(' ')
        for i in range(len(data) - 1):
            FROM.append(eval(data[i]))
            TO.append(eval(data[i + 1]))

print(len(FROM))
print(len(TO))

dialogs = {}

with open('movie_lines.tsv', buffering=1000, encoding="utf8") as f:
    for row in f:
        data = row.split('	')
        dialogs[data[0].strip('"')] = data[-1][:-1].strip('"').replace('""', '').replace('-', '').replace('  ',
                                                                                                          ' ').strip()

IN = []
OUT = []

for i in range(len(FROM)):
    IN.append(dialogs[FROM[i]] + '\n')
    OUT.append(dialogs[TO[i]] + '\n')

train_IN = IN[:int(len(IN) * 0.9)]
train_OUT = IN[:int(len(OUT) * 0.9)]

evaluate_IN = IN[int(len(IN) * 0.9):]
evaluate_OUT = OUT[int(len(OUT) * 0.9):]

test_IN = evaluate_IN[:int(len(evaluate_IN) / 1.5)]
test_OUT = evaluate_OUT[:int(len(evaluate_OUT) / 1.5)]

validation_IN = evaluate_IN[int(len(evaluate_IN) / 1.5):]
validation_OUT = evaluate_OUT[int(len(evaluate_OUT) / 1.5):]

print(len(train_IN), len(evaluate_IN), len(train_IN), len(validation_IN))

with open('train.from', 'w', encoding="utf8") as f:
    f.writelines(train_IN)
with open('train.to', 'w', encoding="utf8") as f:
    f.writelines(train_OUT)

with open('tst2013.from', 'w', encoding="utf8") as f:
    f.writelines(test_IN)
with open('tst2013.to', 'w', encoding="utf8") as f:
    f.writelines(test_OUT)

with open('tst2012.from', 'w', encoding="utf8") as f:
    f.writelines(validation_IN)
with open('tst2012.to', 'w', encoding="utf8") as f:
    f.writelines(validation_OUT)
