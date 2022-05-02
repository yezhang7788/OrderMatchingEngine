# OrderMatchingEngine
## Usage of Code
One can refer to the notebook file for outputs and function version of solution. One py file is function version of solution, the other is class version solution.
## Logic
### Function Solution
#### Work Flow
![PME-workflow-chart](https://user-images.githubusercontent.com/104744511/166203687-97ddb743-f340-4a04-ace4-fbe3da7095cb.png)

Firstly, the records need to be screened for legit order volume. If volume larger than 1000000, the record will be labeled as Reject, then appended to output dataframe. As for legit volume records, it will be labeled as Ack, then appended to output df. Enabling the engine to deal with multiple stock tickers, I created a dictionary, storing tickers as keys, and corresponding buy and sell orders waiting to be executed stored into list as values, called buy/sell waiting queue. After changing the input csv file into a list, with each item representing one dataframe row, the matching engine is reading in the first item in the list each time, mimicing the inflow of one new order made to the exchange. There will be an empty buy order waiting queue and an empty sell order waiting queue for each ticker at the beginning, and each order that is not executed in full or at all will be inserted into the queue according to time priority. And there will be an empty fill output list at the beginning, each order filled will be append to that list. 
The work flow will be, judge if the new order is buy order? Yes, then judge if the waiting sell order queue empty? If yes, then the buy order will be appended to the buy order queue and that new order will be removed from the input order list; if no, we need to apply the price matching logic to match new order with the order in the sell waiting queue. After price matching, we found the order could be matched with the new order, the execution volume will the lower volume between the two. At last, for both the orders in the new order input list and the sell queue, if it is executed in all, the it is removed from the list; if not, the item volume will be modified and remain in the list to be executed next time. 
Until now the work flow for one input order is done, the engine will treat the top item in the input order list as another new input and the work flow begine again, until the input order list is empty. The logic for new sell order is the same but with opposite directions. At last the ouput fill list will be returned with all filled records. 
#### Price Matching Mechanism
This part dives into the price matching mechanism in detail. Since both buy and sell orders have similar logic but only opposite directions, I will use buy order as an example. We need to fide matching price and volume for this new buy order in the sell order waiting queue. There are several senarios need to be treated seperately.  
1 New order is MKT order, there is NO market order in the buy waiting queue, there is NO market order in the sell waiting queue. trade price: lowest price in the sell queue; volume: smaller volume between two orders.  
2 New order is MKT order, there is NO market order in the buy waiting queue, there is market order in the sell waiting queue.  
2.1 If there are all market orders in sell waiting queue, no trade happends.  
2.2 If there is limit order in sell waiting queue, trade price: lowest price in the sell queue limit orders; volume: smaller volume between two orders.  
3 New order is MKT order, there is market order in the buy waiting queue, no trade happens.  
4 New order is LIMIT order, there is NO market order in the buy waiting queue, there is NO market order in the sell waiting queue.  
4.1 If the lowest price in the sell limit order queue if smaller than the buy order price, trade price: new buy order price; volume: smaller volume between two orders.  
4.2 If the lowest price in the sell limit order queue if larger than the buy order price, no trade happens.  
5 New order is LIMIT order, there is NO market order in the buy waiting queue, there is market order in the sell waiting queue. trade price: new order price; volume: smaller volume between two orders.  
6 New order is LIMIT order, there is market order in the buy waiting queue, there is market order in the sell waiting queue. Then the two market orders in the sell and buy waiting queues need to be traded first. trade price: new order price; volume: smaller volume between two market orders.  
7 New order is LIMIT order, there is market order in the buy waiting queue, there is NO market order in the sell waiting queue. No trade happens.  
### Class Solution
The entire work flow could be encapsulated into a class. The object will be the entire workflow. The attribution of the class will be, the input data, the order matching process, the output dataframe.
## Future Improvement
Given limited energy and time working on this, I am aware that the status of the matching engine is far from perfect but a sketch of logic at this stage, there are several points I will make efforts to improve if given more resource to work on this:  
1 The code efficiency and cleaness could be improved in the functions. For example, some scenarios could be combined since processing is similar.  
2 The design of class could be variant and given more thinking.  
3 Need to come up with more edge cases to test the engine. Even if the given samples were tested and passed, there might be untested scenarios that leads of failure of the system.  
