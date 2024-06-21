USE [ApartmentsxCrimeDWH]
GO
/****** Object:  Table [dbo].[DimApartment]    Script Date: 9 cze 2024 10:04:06 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimApartment](
	[ApartmentID] [bigint] IDENTITY(1,1) NOT NULL,
	[ApartmentSourceID] [bigint] NOT NULL,
	[ValidFromDate] [datetime] NOT NULL,
	[ValidToDate] [datetime] NOT NULL,
	[ActiveFlag] [bit] NOT NULL,
	[SquareFeet] [numeric](18, 0) NOT NULL,
	[Bedrooms] [int] NOT NULL,
	[Bathrooms] [numeric](2, 1) NOT NULL,
	[allowsDogs] [bit] NOT NULL,
	[allowsCats] [bit] NOT NULL,
	[hasPhoto] [bit] NOT NULL,
	[hasParking] [bit] NOT NULL,
	[hasGym] [bit] NOT NULL,
	[hasPool] [bit] NOT NULL,
	[hasDishwasher] [bit] NOT NULL,
	[hasElevator] [bit] NOT NULL,
	[hasStorage] [bit] NOT NULL,
	[hasFireplace] [bit] NOT NULL,
 CONSTRAINT [PK_DimApartment] PRIMARY KEY CLUSTERED 
(
	[ApartmentID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DimDate]    Script Date: 9 cze 2024 10:04:06 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimDate](
	[Date] [datetime] NOT NULL,
	[DayNumber] [tinyint] NOT NULL,
	[DaySuffix] [char](2) NOT NULL,
	[WeekDayNumber] [tinyint] NOT NULL,
	[WeekDayName] [varchar](10) NOT NULL,
	[WeekendFlag] [bit] NOT NULL,
	[MonthNumber] [tinyint] NOT NULL,
	[MonthName] [varchar](10) NOT NULL,
	[QuarterNumber] [tinyint] NOT NULL,
	[QuarterName] [varchar](6) NOT NULL,
	[YearNumber] [int] NOT NULL,
	[Hours] [int] NOT NULL,
	[Minutes] [int] NOT NULL,
	[DateID] [int] IDENTITY(1,1) NOT NULL,
 CONSTRAINT [PK_DimDate] PRIMARY KEY CLUSTERED 
(
	[DateID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DimLocation]    Script Date: 9 cze 2024 10:04:06 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DimLocation](
	[LocationID] [bigint] IDENTITY(1,1) NOT NULL,
	[PremisName] [nvarchar](300) NULL,
	[Latitude] [numeric](7, 4) NOT NULL,
	[Longtitude] [numeric](7, 4) NOT NULL,
	[PoliceStationAssignedCode] [char](2) NULL,
 CONSTRAINT [PK_DimLocation] PRIMARY KEY CLUSTERED 
(
	[LocationID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[FactCrimeEvent]    Script Date: 9 cze 2024 10:04:06 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FactCrimeEvent](
	[LocationID] [bigint] NOT NULL,
	[EventKey] [int] NOT NULL,
	[DateID] [int] NOT NULL,
	[CrimeName] [nvarchar](300) NOT NULL,
	[CrimeCommitedNumber] [int] NULL,
	[CrimeCommitedSeverityNumber] [int] NOT NULL,
	[WeaponName] [nvarchar](300) NULL,
	[VictimSex] [char](1) NOT NULL,
	[VictimDescent] [char](1) NOT NULL,
	[VictimAge] [int] NOT NULL,
	[Status] [nvarchar](50) NOT NULL,
	[CrimeEventID] [int] IDENTITY(1,1) NOT NULL,
 CONSTRAINT [PK_FactCrimeEvent] PRIMARY KEY CLUSTERED 
(
	[CrimeEventID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[FactRentalOffer]    Script Date: 9 cze 2024 10:04:06 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FactRentalOffer](
	[OfferID] [bigint] IDENTITY(1,1) NOT NULL,
	[ApartmentID] [bigint] NOT NULL,
	[LocationID] [bigint] NOT NULL,
	[DateID] [int] NOT NULL,
	[Price] [money] NOT NULL,
	[Currency] [char](3) NOT NULL,
	[Source] [nvarchar](50) NOT NULL,
	[PaymentFrequency] [nvarchar](50) NOT NULL,
 CONSTRAINT [PK_FactRentalOffer] PRIMARY KEY CLUSTERED 
(
	[OfferID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[FactCrimeEvent]  WITH CHECK ADD  CONSTRAINT [FK_FactCrimeEvent_DimDate] FOREIGN KEY([DateID])
REFERENCES [dbo].[DimDate] ([DateID])
GO
ALTER TABLE [dbo].[FactCrimeEvent] CHECK CONSTRAINT [FK_FactCrimeEvent_DimDate]
GO
ALTER TABLE [dbo].[FactCrimeEvent]  WITH CHECK ADD  CONSTRAINT [FK_FactCrimeEvent_DimLocation] FOREIGN KEY([LocationID])
REFERENCES [dbo].[DimLocation] ([LocationID])
GO
ALTER TABLE [dbo].[FactCrimeEvent] CHECK CONSTRAINT [FK_FactCrimeEvent_DimLocation]
GO
ALTER TABLE [dbo].[FactRentalOffer]  WITH CHECK ADD  CONSTRAINT [FK_FactRentalOffer_DimApartment] FOREIGN KEY([ApartmentID])
REFERENCES [dbo].[DimApartment] ([ApartmentID])
GO
ALTER TABLE [dbo].[FactRentalOffer] CHECK CONSTRAINT [FK_FactRentalOffer_DimApartment]
GO
ALTER TABLE [dbo].[FactRentalOffer]  WITH CHECK ADD  CONSTRAINT [FK_FactRentalOffer_DimDate] FOREIGN KEY([DateID])
REFERENCES [dbo].[DimDate] ([DateID])
GO
ALTER TABLE [dbo].[FactRentalOffer] CHECK CONSTRAINT [FK_FactRentalOffer_DimDate]
GO
ALTER TABLE [dbo].[FactRentalOffer]  WITH CHECK ADD  CONSTRAINT [FK_FactRentalOffer_DimLocation] FOREIGN KEY([LocationID])
REFERENCES [dbo].[DimLocation] ([LocationID])
GO
ALTER TABLE [dbo].[FactRentalOffer] CHECK CONSTRAINT [FK_FactRentalOffer_DimLocation]
GO
