/*
after importing Pervalence and Pevalence Storke excel sheet in am_HTexas_all_temp

delete FROM [test].[UTHOUSTON\amoosa1].[am_HTexas_all_temp]
  where DataSource like '%wt%'

update [test].[UTHOUSTON\amoosa1].[am_HTexas_all_temp]
	set [Mean Ratio] = [All Ratio]

delete from [test].[UTHOUSTON\amoosa1].[am_HTexas_all_temp]
 where year = 2018

delete from [test].[UTHOUSTON\amoosa1].[am_HTexas_all_temp]
 where year = 2019


*/
--select * from [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
------------------------------------------------------------------------------------------------------------------

declare @Zip3 varchar(50)
declare @PopulationGroup varchar(50)
declare @Category varchar(50)
declare @Measure varchar(50)
declare @StateMeasure varchar(50)
declare @Year int = 2017
declare @StateValue float
declare @ProcessTable table(DataSource varchar(50), Year int, State varchar(50), Zip3 varchar(50), TotalCount int, PopulationGroup varchar(50), Category varchar(50), 
							Measure varchar(50), MeanRatio float, StateMeasure varchar(50), StateValue float, [All] float, AllRatio float,
							MeanRatio_Calc float, StateValue_Calc float, All_Calc float, AllRatio_Calc float, nSum int)

declare @ResultTable table(DataSource varchar(50), Year int, State varchar(50), Zip3 varchar(50), PopulationGroup varchar(50), Category varchar(50), 
							Measure varchar(50), MeanRatio float, StateMeasure varchar(50), StateValue float, [All] float, AllRatio float)
							
drop table if exists [UTHOUSTON\amoosa1].[am_HTexas_WT] 

declare cur cursor for 
	select distinct Zip_3, PopulationGroup, Category, Measure, [State Measure]
		from [UTHOUSTON\amoosa1].[am_HTexas_all]
		where Year = @Year	
		    --and Zip_3 is null
			--and (Zip_3 in ('750', '751') or Zip_3 is NULL)
			--and PopulationGroup= 'all' 
			--and Category='Cost' 
			--and Measure= 'drug cost' 
			--and [State Measure] = 'State Average $'
		order by Zip_3, PopulationGroup, Category, Measure, [State Measure]

open cur 
fetch next from cur into @Zip3, @PopulationGroup, @Category, @Measure, @StateMeasure

while @@FETCH_STATUS = 0
begin
 
			
	insert into @ProcessTable(DataSource, Year, State, Zip3, TotalCount, PopulationGroup, Category, Measure, MeanRatio, StateMeasure, StateValue, [All], AllRatio)
		select distinct a.DataSource, Year, State,  isnull(Zip_3, 'All') as Zip3, 
				(select top 1 TotalCount 
					from [UTHOUSTON\amoosa1].am_HTexas_Zip3_Agg z  
					where z.Zip3 = isnull(a.Zip_3, 'All') 
						and z.DataSource = a.DataSource
						and z.DataYear = @Year) as TotalCount, 
				PopulationGroup, Category, Measure, [Mean Ratio], [State Measure], [State Value], [All], [All Ratio] 
			from [UTHOUSTON\amoosa1].[am_HTexas_all] a			
			where Year = @Year
				and isnull(a.Zip_3, '') = isnull(@Zip3, '')
				and PopulationGroup = @PopulationGroup
				and Category = @Category
				and Measure = @Measure
				and [State Measure] = @StateMeasure
				
	update @ProcessTable
		set nSum = (select sum(TotalCount) from @ProcessTable)

	update @ProcessTable
		set MeanRatio_Calc = (MeanRatio * TotalCount) / nSum,
			StateValue_Calc = (StateValue * TotalCount) / nSum,
			All_Calc = ([All] * TotalCount) / nSum,
			AllRatio_Calc = (AllRatio * TotalCount) / nSum
			
	insert into @ResultTable(DataSource, Year, State, Zip3, PopulationGroup, Category, Measure, 
							 MeanRatio, StateMeasure, StateValue, [All], AllRatio)	
		select 'All', Year, State, Zip3, PopulationGroup, Category, Measure, 
				sum(MeanRatio_Calc) as MeanRatio_Calc, StateMeasure, sum(StateValue_Calc) as StateValue_Calc,
				sum(All_Calc) as All_Calc, sum(AllRatio_Calc) as AllRatio_Calc
			from @ProcessTable
			group by Year, State, Zip3, PopulationGroup, Category, Measure, StateMeasure
		
	--override state value for all zip codes
	set @StateValue = (select top 1 StateValue 
						from @ResultTable 
						where Year = @Year
							and Zip3 = 'All'
							and PopulationGroup = @PopulationGroup
							and Category = @Category
							and Measure = @Measure
							and StateMeasure = @StateMeasure)	
		
	update @ResultTable
		set StateValue = @StateValue
		where Year = @Year
			and Zip3 <> 'All'
			and PopulationGroup = @PopulationGroup
			and Category = @Category
			and Measure = @Measure
			and StateMeasure = @StateMeasure

	--select * from @ProcessTable
	
	delete @ProcessTable
	fetch next from cur into @Zip3, @PopulationGroup, @Category, @Measure, @StateMeasure
end
close cur
deallocate cur

select * 
	into [UTHOUSTON\amoosa1].[am_HTexas_WT] 
	from @ResultTable



--for export to excel
select * 
	into [UTHOUSTON\amoosa1].[am_HTexas_WT_2017_result] 
	from [UTHOUSTON\amoosa1].[am_HTexas_WT] 
	order by PopulationGroup, Category, Measure, Zip3
