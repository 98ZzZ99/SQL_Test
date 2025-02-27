# SQL_Test

I devide the work into two stages. In the first stage, I focused on generating JSON commands with LLM’s help and using tools to calculate SQL. In the second stage, I split the original tools (like KPITools) into flexible arithmetic operations and even tried calling LLM again to optimize the JSON commands.

Since I was constantly adjusting and rewriting the code, many older versions were lost. I tried to recover the first stage’s results, the main program Review_03_04.py and its tool function Review_Tools_03_01.py, but they still need some modifications to run correctly.

For the second stage, my goal was to optimize the code. 
The first objective was to change the KPI function into a division calculation: divide the specified number of workpieces (Number) by the working time, where working time is obtained by converting End_Time and Start_Time to minutes and subtracting them. I thought this would be more flexible, but it ended up causing many issues and didn’t work as expected. 
The second plan was to modify the JSON command by calling LLM twice to adjust the operation target or calculation method (like handling synonyms or case differences), but this led to severe hallucination problems. 
In the end, the main program SQL_main_2_3.py—after removing the second LLM call—still can’t use the division operation smoothly for KPI calculation, likely due to issues with processing intermediate results.
