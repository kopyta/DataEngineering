USE [AdventureWorksKOLOS]
GO
/****** Object:  Table [dbo].[DimDate]    Script Date: 25.04.2024 16:34:33 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimDate](
	[DateKey] [bigint] NOT NULL,
	[Date] [date] NOT NULL,
	[WeekDayName] [varchar](10) NOT NULL,
	[WeekendFlag] [char](3) NOT NULL,
	[HolidayText] [nvarchar](100) NOT NULL,
	[HolidayFlag] [char](3) NOT NULL,
	[MonthNumber] [tinyint] NOT NULL,
	[MonthName] [varchar](10) NOT NULL,
	[QuarterNumber] [tinyint] NOT NULL,
	[QuarterName] [varchar](6) NOT NULL,
	[YearNumber] [int] NOT NULL,
 CONSTRAINT [PK_DimDate] PRIMARY KEY CLUSTERED 
(
	[Date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DimDepartment]    Script Date: 25.04.2024 16:34:33 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimDepartment](
	[DepartmentID] [int] NOT NULL,
	[Name] [nvarchar](50) NOT NULL,
	[GroupName] [nvarchar](50) NOT NULL,
	[ModifiedDate] [date] NOT NULL,
 CONSTRAINT [PK_DimDepartment] PRIMARY KEY CLUSTERED 
(
	[DepartmentID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DimEmployee]    Script Date: 25.04.2024 16:34:33 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimEmployee](
	[EmployeeID] [int] NOT NULL,
	[FullName] [nvarchar](200) NOT NULL,
	[OrganizationNode] [hierarchyid] NOT NULL,
	[OrganizationLevel] [int] NOT NULL,
	[JobTitle] [nvarchar](50) NOT NULL,
	[BirthDate] [date] NOT NULL,
	[MartialStatus] [nchar](1) NOT NULL,
	[Gender] [nchar](1) NOT NULL,
	[HireDate] [date] NOT NULL,
	[VacationHours] [int] NOT NULL,
	[SickLeaveHours] [int] NOT NULL,
	[EmploymentStatus] [nchar](1) NOT NULL,
	[EmailAddress] [nvarchar](50) NULL,
	[ModifiedDate] [date] NOT NULL,
 CONSTRAINT [PK_DimEmployee] PRIMARY KEY CLUSTERED 
(
	[EmployeeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[FactEmployeeShift]    Script Date: 25.04.2024 16:34:33 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FactEmployeeShift](
	[ShiftID] [int] NOT NULL,
	[EmployeeID] [int] NOT NULL,
	[DepartmentID] [int] NOT NULL,
	[StartTime] [date] NOT NULL,
	[EndTime] [date] NOT NULL,
	[PayRate] [money] NOT NULL,
	[WynagrodzenieZaZmiane] [money] NOT NULL,
	[DługośćZmiany] [int] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[FactEmployeeShift]  WITH CHECK ADD  CONSTRAINT [FK_FactEmployeeShift_DimDate] FOREIGN KEY([StartTime])
REFERENCES [dbo].[DimDate] ([Date])
GO
ALTER TABLE [dbo].[FactEmployeeShift] CHECK CONSTRAINT [FK_FactEmployeeShift_DimDate]
GO
ALTER TABLE [dbo].[FactEmployeeShift]  WITH CHECK ADD  CONSTRAINT [FK_FactEmployeeShift_DimDate1] FOREIGN KEY([EndTime])
REFERENCES [dbo].[DimDate] ([Date])
GO
ALTER TABLE [dbo].[FactEmployeeShift] CHECK CONSTRAINT [FK_FactEmployeeShift_DimDate1]
GO
ALTER TABLE [dbo].[FactEmployeeShift]  WITH CHECK ADD  CONSTRAINT [FK_FactEmployeeShift_DimDepartment] FOREIGN KEY([DepartmentID])
REFERENCES [dbo].[DimDepartment] ([DepartmentID])
GO
ALTER TABLE [dbo].[FactEmployeeShift] CHECK CONSTRAINT [FK_FactEmployeeShift_DimDepartment]
GO
ALTER TABLE [dbo].[FactEmployeeShift]  WITH CHECK ADD  CONSTRAINT [FK_FactEmployeeShift_DimEmployee] FOREIGN KEY([EmployeeID])
REFERENCES [dbo].[DimEmployee] ([EmployeeID])
GO
ALTER TABLE [dbo].[FactEmployeeShift] CHECK CONSTRAINT [FK_FactEmployeeShift_DimEmployee]
GO
