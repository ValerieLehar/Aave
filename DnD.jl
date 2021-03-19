using Pkg


Pkg.add("DataFrames")
Pkg.add("CSV")
Pkg.add("JLD2")
Pkg.add("FileIO")
Pkg.add("Decimals")
Pkg.add("Plots")
Pkg.add("Statistics")
Pkg.add("InvertedIndices")
Pkg.add("SharedArrays")
Pkg.add("TableView")
Pkg.add("JSON")
Pkg.add("ColorSchemes")
Pkg.add("VegaLite")
Pkg.add("Dates")
Pkg.add("GLM")


using DataFrames
using CSV
using JLD2, FileIO
using Decimals
using Plots
using Statistics
using InvertedIndices
using ColorSchemes
using Dates
using GLM
using VegaLite

#deposits=CSV.read("E:/CloudProjects/Valerie/DungeonsDataAave/Copy of v1_deposits.csv",DataFrame)
deposits=CSV.read("I:/scratch/Aave/Copy of v1_deposits.csv",DataFrame) #deposit 
deposits2=CSV.read("I:/scratch/Aave/Copy of v2_deposit.csv",DataFrame) #deposit2
redeem=CSV.read("I:/scratch/Aave/Copy of v1_redeem_underlying.csv",DataFrame) #redeem
redeem2=CSV.read("I:/scratch/Aave/Copy of v2_withdraw.csv",DataFrame) #redeem2

unique(deposits.logged__reserve) #22 tokens
unique(redeem.logged__reserve) #22 tokens
unique(redeem2.logged_reserve) #24 tokens

names(deposits) # collumns 
names(redeem)  
names(deposits2)
names(redeem2)

select!(deposits, Not(:logged__referral)) #Deleting extra collumns
select!(deposits, Not(:logged__timestamp)) 
select!(deposits2, Not([:logged_referral, :logged_onbehalfof])) 
select!(redeem, Not(:logged__timestamp)) 
select!(redeem2, Not(:logged_to)) 

rename!(deposits, :logged__reserve => :logged_reserve) #rename
rename!(deposits, :logged__user => :logged_user)
rename!(deposits, :logged__amount => :logged_amount)

rename!(redeem, :logged__reserve => :logged_reserve) #rename
rename!(redeem, :logged__user => :logged_user)
rename!(redeem, :logged__amount => :logged_amount)

redeem12=vcat(redeem, redeem2) #merges redeems
deposits12=vcat(deposits, deposits2) #merges deposits

redeem12.logged_amount = -redeem12.logged_amount #positive to negative

#tx = transactions
tx3=vcat(deposits12, redeem12) #merges deposites and redeems

tx3.block_signed_at = DateTime.(tx3.block_signed_at, "y-m-d H:M:S")

sort!(tx3, [:logged_reserve, :block_signed_at] ) #sort by currency type and date
gtx=groupby(tx3, :logged_reserve) #group by token
transform!(gtx, :logged_amount => cumsum => :bal) #do the math to get ballance

@load "I:/scratch/Aave/dftokensV3.jld" #load in dads name generator

select!(dftokensV3,[:addr, :ticker_symbol, :decimals, :name]) #renaming tokens
rename!(dftokensV3, :addr => :logged_reserve)
dftokensV3.logged_reserve = "0x" .* dftokensV3.logged_reserve

tx4=leftjoin(tx3, dftokensV3, on= :logged_reserve) #joines the name labeling with tx3
tx4[:,6:end]

ix=findall(ismissing.(tx4.ticker_symbol).==1) #missing labels identifying and renaming
tx4.ticker_symbol[ix].="xSUSHI"
tx4.decimals[ix].="18"
tx4.name[ix].="SushiBar"

ix=findall(tx4.decimals.=="") #Missing decimals identifying and renaming
tx4[ix, 6:end]
tx4[ix, :]
tx4.ticker_symbol[ix].="WETH"
tx4.decimals[ix].="18"
tx4.name[ix].="Ether"

tx4.decimals=parse.(Int64,tx4.decimals) #Decimal string => decimal Int64

tx4.bal2 = tx4.bal ./ 10 .^ tx4.decimals #calculates the value using the decimals
tx4[:,6:end]
unique(tx4.ticker_symbol)

#cleanup v
deposits=nothing
deposits2=nothing
redeem=nothing
redeem2=nothing
redeem12=nothing
deposits12=nothing
tx3=nothing

prices=CSV.read("I:/scratch/Aave/prices2.csv",DataFrame) #bring in prices

tx4.blockday=Date.(tx4.block_signed_at) #just talks about the day instead of day and time

rename!(prices, :tok => :ticker_symbol) #rename
rename!(prices, :date => :blockday)

ix=findall(ismissing.(prices.blockday).==1) #finding mising blockday lines
prices[ix,:]
ix=findall(ismissing.(prices.blockday).==0) #finding all lines where date is present
prices=prices[ix,:]


tx4=leftjoin(tx4, prices, on= [:ticker_symbol, :blockday])
tx4.bal_loanUSD = tx4.bal2 .* tx4.price

##############################################################################################################################
#graph v

tx5=combine(groupby(tx4, [:ticker_symbol, :blockday]), :bal_loanUSD => last => :balanceUSD)

plot1=(tx5 |> #graph
@vlplot(:line,width=800, height=400,
    x={:blockday,axis={format="%B-%Y", title="Date"}},
	#y=:totalvol,
	y={:balanceUSD, axis={title="Deposit Balance (USD)"}},
    color=:ticker_symbol #adding a color makes a stacked field by default
))
save("I:/scratch/Aave/graph-deposits.png", plot1)

#Above^ all of the reedeems and the deposits organized and together
#########################################################################################################################################
#Belowv all of the Loans and repayments

borrow1=CSV.read("I:/scratch/Aave/Copy of v1_borrow.csv",DataFrame) #inserting data
repay1=CSV.read("I:/scratch/Aave/Copy of v1_repay.csv",DataFrame)
borrow2=CSV.read("I:/scratch/Aave/Copy of v2_borrow.csv",DataFrame)
repay2=CSV.read("I:/scratch/Aave/Copy of v2_repay.csv",DataFrame)

names(borrow1) #Collums
names(borrow2)
names(repay1)
names(repay2)

rename!(repay1, :logged__reserve => :logged_reserve) #rename
rename!(repay1, :logged__user => :logged_user) 
rename!(repay1, :logged__repayer => :logged_repayer) 
rename!(repay1, :logged__amountminusfees => :logged_amount)

rename!(borrow1, :logged__reserve => :logged_reserve) #rename
rename!(borrow1, :logged__user => :logged_user)
rename!(borrow1, :logged__amount => :logged_amount)

select!(repay1, Not(:logged__fees)) #Deleting extra collums
select!(repay1, Not(:logged__borrowbalanceincrease))
select!(repay1, Not(:logged__timestamp))
select!(borrow1, Not(:logged__timestamp))
select!(borrow1, Not(:logged__originationfee))
select!(borrow1, Not(:logged__borrowbalanceincrease))
select!(borrow1, Not(:logged__referral))
select!(borrow2, Not(:logged_referral))
select!(borrow1, Not(:logged__borrowratemode))
select!(borrow1, Not(:logged__borrowrate))
select!(borrow2, Not(:logged_borrowratemode))
select!(borrow2, Not(:logged_borrowrate))
select!(borrow2, Not(:logged_onbehalfof))

repay12=vcat(repay1, repay2) #merging repays
borrow12=vcat(borrow1, borrow2) #merging borrows

select!(repay12, Not(:logged_repayer)) #deleting repayer cuz i forgot to before

repay12.logged_amount = -repay12.logged_amount #positive to negative

repay12[:, 6:end] #viewing

names(borrow12) # Collums
names(repay12)

loan_tx=vcat(borrow12, repay12) #merges borrows and repays

loan_tx.block_signed_at = DateTime.(loan_tx.block_signed_at, "y-m-d H:M:S") #just day instead of time too

sort!(loan_tx, [:logged_reserve, :block_signed_at]) #sort by token and date

g_loan_tx=groupby(loan_tx, :logged_reserve) #group by token
transform!(g_loan_tx, :logged_amount => cumsum => :bal_loan) #do the math to get ballance

@load "I:/scratch/Aave/dftokensV3.jld" #load in dads name generator

select!(dftokensV3,[:addr, :ticker_symbol, :decimals, :name]) #renaming tokens
rename!(dftokensV3, :addr => :logged_reserve)
dftokensV3.logged_reserve = "0x" .* dftokensV3.logged_reserve

loan_tx2=leftjoin(loan_tx, dftokensV3, on= :logged_reserve) #joines the name labeling with loan_tx
loan_tx2[:,6:end]

ix2=findall(ismissing.(loan_tx2.ticker_symbol).==1) #missing labels identifying and renaming

loan_tx2[ix2, 6:end] #finsing the logged reserve code for the missing token
loan_tx2[ix2, :]
loan_tx2[ix2, :logged_reserve] #put the code into either scan and its sushi

loan_tx2.ticker_symbol[ix2].="xSUSHI"
loan_tx2.decimals[ix2].="18"
loan_tx2.name[ix2].="SushiBar"

ix2=findall(loan_tx2.decimals.=="") #Missing decimals identifying and renaming
loan_tx2[ix2, 6:end]
loan_tx2[ix2, :]
loan_tx2.ticker_symbol[ix2].="WETH"
loan_tx2.decimals[ix2].="18"
loan_tx2.name[ix2].="Ether"

loan_tx2[:, 6:end] #sees that the decimal collum is a string
loan_tx2.decimals=parse.(Int64,loan_tx2.decimals) #string => Int64
loan_tx2.bal_loan = loan_tx2.bal_loan ./ 10 .^ loan_tx2.decimals #converts to correct decimal

#cleanup
loan_tx=nothing
borrow1=nothing
borrow2=nothing
borrow12=nothing
repay1=nothing
repay2=nothing
repay12=nothing

prices=CSV.read("I:/scratch/Aave/prices2.csv",DataFrame) #bring in prices

loan_tx2.blockday=Date.(loan_tx2.block_signed_at) #just talks about the day instead of day and time

rename!(prices, :tok => :ticker_symbol) #rename
rename!(prices, :date => :blockday)

ix=findall(ismissing.(prices.blockday).==1) #find missing blockday lines
prices[ix,:]
ix=findall(ismissing.(prices.blockday).==0) #find all lines where date is present
prices=prices[ix,:]

loan_tx3=leftjoin(loan_tx2, prices, on= [:ticker_symbol, :blockday])  #join the thing with the thing
loan_tx3.bal_loanUSD = loan_tx3.bal_loan .* loan_tx3.price #loan ballance into US dollars

#################################################################################################################
#graph v

loan_tx4=combine(groupby(loan_tx3, [:ticker_symbol, :blockday]), :bal_loanUSD => last => :balanceUSD) #


plot2=(loan_tx4 |> #ploting graph
@vlplot(:line,width=800, height=400,
    x={:blockday,axis={format="%B-%Y", title="Date"}},
    y={:balanceUSD, axis={title="Loan Balance (USD)"}},
    color=:ticker_symbol
))

save("I:/scratch/Aave/graph-loans.png", plot2)
########################################################################################################################
ru=CSV.read("I:/scratch/Aave/Copy of v1_reserve_updated.csv",DataFrame) 
ru[:,6:end]