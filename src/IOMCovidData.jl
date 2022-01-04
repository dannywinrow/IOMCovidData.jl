module IOMCovidData

using PDFIO, DataFrames, Dates, CSV, HTTP, EzXML

export getPDFs, processPDFs, updatedatacsv

# A routine to extract data from the IOM Government pdf snapshots

function getPDFs(folder=""; overwrite = false)
    urlstem = "https://covid19.gov.im"
    urlpagearch = "/general-information/latest-updates/archived-covid-snapshots/"
    urlpagecurr = "/general-information/latest-updates/"

    function innergetpdfs(urlpage)
        url = urlstem*urlpage
        r = HTTP.request("GET", url)

        # read the body into a String
        html = root(parsehtml(String(r.body)))
        xpath = "//a"
        link = findall(xpath, html)
        
        urls = hcat(getproperty.(link,:content),urlstem .* getindex.(link,"href"))
        pdfurls = urls[occursin.(r"\d{1,2}\s\w+\s\d{4}",urls[:,1]),:]
        #filter by pdf, but no need as date format works
        #pdfurls = urls[occursin.("pdf",urls[:,2]),:]

        pdfurls = hcat(Date.(getindex.(match.(r"(\d{1,2}\s\w+\s\d{4})",pdfurls[:,1]),1), "d U Y"), pdfurls[:,2])
        pdfurls = hcat(pdfurls, Dates.format.(pdfurls[:,1],"yyyy-mm-dd") .* ".pdf")
        function downloadpdf(url, filename)
            filepath = joinpath(folder,filename)
            if overwrite || !isfile(filepath)
                download(url,filepath)
                filepath
            end
        end
        downloadpdf.(pdfurls[:,2],pdfurls[:,3])
    end

    innergetpdfs(urlpagecurr)
    innergetpdfs(urlpagearch)

end

function processPDFs(folder; firstpdf = "2021-07-29.pdf")
    
    #read in the files in the folder
    lst = readdir(folder)

    #select only valid files for processing
    prolst = lst[lst .>= firstpdf]
    filter!(x->occursin(r"\d{4}-\d{2}-\d{2}.pdf$",x),prolst)

    #initialise variables
    io = IOBuffer()
    testingOutput = Vector[]
    frontOutput = Vector[]

    #loop through each file for processing
    for pdname in prolst
        
        #open pdf file for reading
        worked = true
        @info "Processing: " filename=joinpath(folder,pdname)
        p = nothing
        try
            p = pdDocOpen(joinpath(folder,pdname))
        catch e
            worked = false    
        end
        if worked

            if pdDocGetPageCount(p) >= 8
                #routine to extract relevant data from page 8
                pdPageExtractText(io,pdDocGetPage(p, 8))
                data = String(take!(io))
                filedate = Date(pdname[1:10])
                date =
                    try
                        Date(match(r"Default Date\s+(\d\d/\d\d/\d\d\d\d)",data).captures[1],"dd/mm/yyyy")
                    catch e
                        0
                    end
                tests =
                    try
                        parse(Int64,replace(match(r"((?:\d|,)+)\s*TESTS",data).captures[1],","=>""))
                    catch e
                        -1
                    end
                concluded =
                    try
                        parse(Int64,replace(match(r"TESTS(?:.|\R)+?((?:\d|,)+)\s*TESTS",data).captures[1],","=>""))
                    catch e
                        -1
                    end
                rate = 
                    try
                        parse(Float64,replace(match(r"((?:\d|\.)+)%",data).captures[1],"%"=>""))
                    catch e
                        -1
                    end
                awaitresult = 
                    try
                        parse(Int64,replace(match(r"((?:\d|,)+)\s*AWAITING RESULT",data).captures[1],","=>""))
                    catch e
                        -1
                    end
                bookedtest =
                    try
                        parse(Int64,replace(match(r"((?:\d|,)+)\s*BOOKED TESTS",data).captures[1],","=>""))
                    catch e
                        -1
                    end
                push!(testingOutput, [filedate, date, tests, concluded, rate, awaitresult, bookedtest])
            end
            
            #routine to extract relevant data from page 1
            pdPageExtractText(io,pdDocGetPage(p, 1))
            data = String(take!(io))

            refreshdate =
                try
                    Date(match(r"Data Last Refreshed\s+(\d\d/\d\d/\d\d\d\d)",data).captures[1],"dd/mm/yyyy")
                catch e
                    0
                end

            topush = String[]
            
            for m in collect(eachmatch(r"(?:\w|\s)(\d(?:\d|,)*)(?:\w|\s)",split(data,"15th Feb 2021")[1]))
                push!(topush, m.captures[1])
            end

            if length(split(data,"15th Feb 2021")) > 1
                for n in length(topush):8
                    push!(topush,"-1")
                end
                for m in collect(eachmatch(r"(?:\w|\s)(\d(?:\d|,)*)(?:\w|\s)",split(data,"15th Feb 2021")[2]))
                    push!(topush, m.captures[1])
                end
            end

            for n in length(topush):22
                push!(topush,"-1")
            end
            topush
            
            
            push!(frontOutput, [filedate, refreshdate, topush...])

            pdDocClose(p)
        end
    end

    frontOutput, testingOutput
end

function getIOMCovidData(folder="data")
    CSV.read(joinpath(folder,"data.csv"),DataFrame)
end

function getJerseyCovidData()
    r = HTTP.request("GET","https://www.gov.je/Datasets/ListOpenData?ListName=COVID19")
    CSV.read(r.body,DataFrame)
end

function getOWIDCovidData()
    r = HTTP.request("GET","https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv")
    CSV.read(r.body,DataFrame)
end

function getUKCovidData()
    data = getOWIDCovidData()
    data[data.location .== "United Kingdom",:]
end

function getAllData()
    data = outerjoin(getIOMCovidData(),getUKCovidData(),on=[:Date=>:date],makeunique=true)
    data = outerjoin(data,getJerseyCovidData(),on=:Date,makeunique=true)
end

function updatedatacsv(folder;update=true,updatemanual=true)
    getPDFs(folder)
    if update
        data = getIOMCovidData(folder)
        lastdate = maximum(data.Date)
        firstpdf = Dates.format(lastdate+Dates.Day(1),"Y-mm-dd")*".pdf"
        frontOutput, testingOutput = processPDFs(folder,firstpdf=firstpdf)
    else
        data = DataFrame()
        frontOutput, testingOutput = processPDFs(folder)
    end
    if length(frontOutput) > 0
        df1 = DataFrame(permutedims(hcat(frontOutput...),(2,1)),[:Date,:RefreshDate,:Community,:ActiveCases,:Hospital,:x5,:x6,:x7,:x8,:x9,:x10,:Investigated,:New,:PendingInvestigation,:Closed,:x15,:x16,:x17,:x18,:x19,:x20,:x21,:x22,:x23,:x24])
        df2 = DataFrame(permutedims(hcat(testingOutput...),(2,1)),[:Date,:DefaultDate,:Tests,:Concluded,:Rate,:Awaiting,:BookedTomorrow])
        df = outerjoin(df1,df2, on=:Date, makeunique=true)
        select!(df,Not(r"^x\d"))
        df[:,:Source] .= "pdf"
        data = vcat(data,df)
    end
    if updatemanual
        manual = CSV.read(joinpath(folder,"manual.csv"),DataFrame)
        manual[:,:Source] .= "manual"
        data = antijoin(data,manual,on=:Date)
        data = vcat(data,manual)
        sort!(data,:Date)
    end

    CSV.write(joinpath(folder,"data.csv"),data)
    #CSV.write(joinpath("data","data.csv"),data)
    @info "Updated data.csv with:" data = data
end

end